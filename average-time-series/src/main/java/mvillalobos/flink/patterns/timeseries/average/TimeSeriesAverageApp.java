package mvillalobos.flink.patterns.timeseries.average;

import com.google.common.collect.ImmutableList;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.jdbc.JDBCOptions;
import org.apache.flink.api.java.io.jdbc.JDBCUpsertTableSink;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "Time Series Average", mixinStandardHelpOptions = true,
        description = "Compute the average of the time series with a 15 minute tumbling event time window and upsert the results into an Apache Derby database.")
public class TimeSeriesAverageApp implements Callable<Integer> {

    private final static Logger logger = LoggerFactory.getLogger(TimeSeriesAverageApp.class);

    @CommandLine.Option(names = {"-f", "--input-file"}, description = "The CSV input file of time series data. Each line must be in the format: String, double, Instant.")
    private File inputFile;

    @Override
    public Integer call() throws Exception {
        stream(inputFile.toString());
        return 0;
    }

    public void stream(String inputFilePath) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        // GIVEN a SOURCE with a CSV input file
        // in which each line has a: String, double, Instant
        // THEN the MAP operator
        // transforms the line into a Tuple7
        // f0: name: String
        // f1: window_size: int
        // f2: value: double
        // f3: event_timestamp: Instant
        // f4: aggregate_sum: double
        // f5: aggregate_count double
        // f6: is_backfile: boolean
        // WHEN the map operation finishes
        // THEN the event time assigned using field f3
        final DataStream<Tuple7<String, Integer, Double, Instant, Double, Integer, Boolean>> timeSeriesStream = env.readTextFile(inputFilePath)
                .map(line -> {
                    final String[] split = line.split(",");
                    final String name = split[0];
                    final double value = Double.parseDouble(split[1]);
                    final Instant timestamp = Instant.parse(split[2]);
                    return Tuple7.of(name, 1, value, timestamp, value, 1, false);
                }).returns(Types.TUPLE(Types.STRING, Types.INT, Types.DOUBLE, TypeInformation.of(Instant.class), Types.DOUBLE, Types.INT, Types.BOOLEAN))
                .name("time series stream")
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<>() {
                            @Override
                            public long extractAscendingTimestamp(Tuple7<String, Integer, Double, Instant, Double, Integer, Boolean> element) {
                                return element.f3.toEpochMilli();
                            }
                        }
                );

        final JDBCUpsertTableSink jdbcUpsertTableSink = buildJdbcUpsertTableSink();

        upsertToJDBC(jdbcUpsertTableSink, timeSeriesStream);

        // GIVEN a data stream with Tuple7
        // f0: name: String
        // f1: window_size: int
        // f2: value: double
        // f3: event_timestamp: Instant
        // f4: aggregate_sum: double
        // f5: aggregate_count double
        // f6: is_backfill: boolean
        // THEN the stream is KEYED BY: f0: name:String, f1: window_size: int
        // THEN the stream is WINDOWED into a tumbling event time window of 15 minutes
        // THEN the window is configured to allow elements late by 1 hour
        // THEN a low-level process window function is applied to the window that
        //      aggregates the time series by assigning the following tuple fields:
        //      f1: window_size = 15 minutes in miliseconds
        //      f2: value = average value in this 15 minute window
        //      f3: event_timestamp = the first epoch millisecond in this 15 minute window
        //      f4: aggregate_sum = sum of f2 values in this 15 minute window
        //      f5: aggregate_count = number of values in this 15 minute window
        final DataStream<Tuple7<String, Integer, Double, Instant, Double, Integer, Boolean>>
                aggregateTimeSeriesStream = timeSeriesStream.keyBy(0, 1)
                .window(TumblingEventTimeWindows.of(Time.minutes(15)))
                .allowedLateness(Time.hours(1))
                .process(new ProcessWindowFunction<Tuple7<String, Integer, Double, Instant, Double, Integer, Boolean>, Tuple7<String, Integer, Double, Instant, Double, Integer, Boolean>, Tuple, TimeWindow>() {
                    @Override
                    public void process(
                            Tuple tuple,
                            Context context,
                            Iterable<Tuple7<String, Integer, Double, Instant, Double, Integer, Boolean>> elements,
                            Collector<Tuple7<String, Integer, Double, Instant, Double, Integer, Boolean>> out
                    ) throws Exception {

                        final Tuple7<String, Integer, Double, Instant, Double, Integer, Boolean> aggregation = new Tuple7<>();

                        boolean is_window_initialized = false;
                        for (Tuple7<String, Integer, Double, Instant, Double, Integer, Boolean> element : ImmutableList.copyOf(elements).reverse()) {

                            if (!is_window_initialized) {

                                final Instant timestamp = Instant.ofEpochMilli(context.window().getStart());

                                aggregation.f0 = element.f0;
                                aggregation.f1 = (int) Time.minutes(15).toMilliseconds();
                                aggregation.f2 = element.f2;
                                aggregation.f3 = timestamp;
                                aggregation.f4 = 0D;
                                aggregation.f5 = 0;
                                aggregation.f6 = false;
                                is_window_initialized = true;
                            }

                            aggregation.f4 += element.f2;
                            aggregation.f5++;
                            aggregation.f2 = aggregation.f4 / aggregation.f5;
                        }

                        logger.info("Added aggregation: {}", aggregation);
                        out.collect(aggregation);
                    }
                }).name("averaged keyed tumbling window event time stream");


        // GIVEN a data-stream of tuple7
        // f0: name: String
        // f1: window_size: int
        // f2: value: double
        // f3: event_timestamp: Instant
        // f4: aggregate_sum: double
        // f5: aggregate_count double
        // f6: is_backfill: boolean
        // THAT was aggregated to compute the average on f2: value: double
        // WITH a grouping of: f0: name:String, f1: window_size: int
        // WITH a tumbling event time window of 15 minutes
        // THEN the stream is KEYED BY: f1: window_size: int
        // THEN a low-level keyed process function is applied to the window that
        //      WHEN the keyed process function opens it
        //          initializes a VALUE STATE of TreeSet<String> called "nameSymbolTable"
        //          initializes a MAP STATE of
        //            KEY of String
        //            VALUE of LinkedPriorityQueue<Tuple7> ranked by f3 where Tuple7 is:
        //              f0: String
        //              f1: int
        //              f2: double
        //              f3: Instant
        //              f4: double
        //              f5: double
        //              f6: boolean
        //            called "adj"
        //          initializes the time series graph called "timeSeriesGraph"
        //      WHEN the keyed process function processes an element it
        //          adds the value to the "timeSeriesGraph"
        //          fires an timer to occur at f3: event_timestamp: Instant + 15 minutes (at the end of a window)
        //      WHEN the keyed process functions coalesced timers are handled it
        //          calculates the current "event_time" to handle which is the timestamp - 15 minutes
        //          finds the elements in the "timeSeriesGraph" and collects them.
        //              this has the side-affect of removing
        //              values in the list from MAP STATE "adj" that are no longer needed.
        final DataStream<Tuple7<String, Integer, Double, Instant, Double, Integer, Boolean>> backfilledAggregateTimeSeriesStream =
                aggregateTimeSeriesStream.keyBy(1)
                        .process(
                                new KeyedProcessFunction<>() {

                                    private ValueState<Set<String>> nameSymbolTable;

                                    private MapState<String, LinkedPriorityQueue<Tuple7<String, Integer, Double, Instant, Double, Integer, Boolean>, Instant>> adj;

                                    private TimeSeriesGraph<Tuple7<String, Integer, Double, Instant, Double, Integer, Boolean>, String, Instant> timeSeriesGraph;

                                    @Override
                                    public void open(Configuration parameters) {
                                        MapStateDescriptor<String, LinkedPriorityQueue<Tuple7<String, Integer, Double, Instant, Double, Integer, Boolean>, Instant>> adjacencyDescriptor =
                                                new MapStateDescriptor<>(
                                                        "time-series-graph",
                                                        TypeInformation.of(String.class),
                                                        TypeInformation.of(new TypeHint<>() {})
                                                );

                                        ValueStateDescriptor<Set<String>> nameSymbolTableDescriptor =
                                                new ValueStateDescriptor<>("name-symbol-table", TypeInformation.of(new TypeHint<>() {}));

                                        adj = getRuntimeContext().getMapState(adjacencyDescriptor);
                                        nameSymbolTable = getRuntimeContext().getState(nameSymbolTableDescriptor);
                                        timeSeriesGraph = new TimeSeriesGraph<>(nameSymbolTable, adj, tuple7 -> tuple7.f0, tuple7 -> tuple7.f3);
                                    }

                                    @Override
                                    public void processElement(
                                            Tuple7<String, Integer, Double, Instant, Double, Integer, Boolean> value,
                                            Context ctx,
                                            Collector<Tuple7<String, Integer, Double, Instant, Double, Integer, Boolean>> out
                                    ) throws Exception {

                                        if (nameSymbolTable.value() == null) {
                                            nameSymbolTable.update(new TreeSet<>());
                                        }

                                        final Instant evenTime = value.f3;
                                        final long timer = evenTime.toEpochMilli() + Time.minutes(15).toMilliseconds();

                                        logger.info(
                                                "processElement with key: {}, value: {}.  registering timer: {}",
                                                ctx.getCurrentKey(),
                                                value,
                                                Instant.ofEpochMilli(timer)
                                        );
                                        ctx.timerService().registerEventTimeTimer(timer);

                                        timeSeriesGraph.add(value);

                                    }

                                    @Override
                                    public void onTimer(
                                            long timestamp,
                                            OnTimerContext ctx,
                                            Collector<Tuple7<String, Integer, Double, Instant, Double, Integer, Boolean>> out
                                    ) throws Exception {

                                        final Instant event_time = Instant.ofEpochMilli(timestamp).minus(15, ChronoUnit.MINUTES);

                                        timeSeriesGraph.findThenCutRemainder(
                                                event_time,
                                                out::collect,
                                                (tuple7, t) -> new Tuple7<>(
                                                    tuple7.f0, tuple7.f1, tuple7.f2, t, tuple7.f4, tuple7.f5, true
                                                )
                                        );
                                    }
                                });

        upsertToJDBC(jdbcUpsertTableSink, backfilledAggregateTimeSeriesStream);

        env.execute("time series");
    }

    private JDBCUpsertTableSink buildJdbcUpsertTableSink() {
        final JDBCUpsertTableSink jdbcUpsertTableSink = JDBCUpsertTableSink.builder()
                .setOptions(JDBCOptions.builder()
                        .setDBUrl("jdbc:derby:memory:flink")
                        .setTableName("time_series")
                        .build())
                .setTableSchema(TableSchema.builder()
                        .field("name", DataTypes.VARCHAR(50).notNull())
                        .field("window_size", DataTypes.INT().notNull())
                        .field("value", DataTypes.DOUBLE().notNull())
                        .field("event_timestamp", DataTypes.TIMESTAMP().notNull())
                        .field("aggregate_sum", DataTypes.DOUBLE().notNull())
                        .field("aggregate_count", DataTypes.INT().notNull())
                        .field("is_backfill", DataTypes.BOOLEAN().notNull())
                        .primaryKey("name", "window_size", "event_timestamp")
                        .build())
                .build();
        jdbcUpsertTableSink.setKeyFields(new String[]{"name", "window_size", "event_timestamp"});
        return jdbcUpsertTableSink;
    }

    private void upsertToJDBC(JDBCUpsertTableSink jdbcUpsertTableSink, DataStream<Tuple7<String, Integer, Double, Instant, Double, Integer, Boolean>> timeSeriesStream) {
        jdbcUpsertTableSink.consumeDataStream(timeSeriesStream.map(t -> {
            final Row row = new Row(7);
            row.setField(0, t.f0);
            row.setField(1, t.f1);
            row.setField(2, t.f2);
            row.setField(3, Timestamp.from(t.f3));
            row.setField(4, t.f4);
            row.setField(5, t.f5);
            row.setField(6, t.f6);
            return new Tuple2<>(true, row);
        }).returns(new TypeHint<Tuple2<Boolean, Row>>() {
        })).name("upsert to JDBC");
    }

    public static void main(String[] args) throws Exception {

        final String databaseURL = "jdbc:derby:memory:flink;create=true";
        int exitCode;
        try (final Connection con = DriverManager.getConnection(databaseURL)) {
            try (final Statement stmt = con.createStatement();) {
                stmt.execute("CREATE TABLE time_series (\n" +
                        "    id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),\n" +
                        "    name VARCHAR(50) NOT NULL,\n" +
                        "    window_size INTEGER NOT NULL DEFAULT 1,\n" +
                        "    event_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
                        "    value DOUBLE PRECISION NOT NULL DEFAULT 0,\n" +
                        "    aggregate_sum DOUBLE PRECISION NOT NULL DEFAULT 0,\n" +
                        "    aggregate_count INTEGER NOT NULL DEFAULT 1,\n" +
                        "    is_backfill BOOLEAN NOT NULL DEFAULT false,\n" +
                        "    version INTEGER NOT NULL DEFAULT 1,\n" +
                        "    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
                        "    modify_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
                        "    UNIQUE (name, window_size, event_timestamp)\n" +
                        ")");
            }

            exitCode = new CommandLine(new TimeSeriesAverageApp()).execute(args);

            try (final Statement stmt = con.createStatement()) {
                final ResultSet rs = stmt.executeQuery("SELECT id, name, window_size, event_timestamp, value, aggregate_sum, aggregate_count, is_backfill, version, create_time, modify_time FROM time_series ORDER BY window_size, event_timestamp, name");
                while (rs.next()) {
                    final long id = rs.getLong(1);
                    final String name = rs.getString(2);
                    final int window_size = rs.getInt(3);
                    final Timestamp event_timestamp = rs.getTimestamp(4);
                    final double value = rs.getDouble(5);
                    final double aggregate_sum = rs.getDouble(6);
                    final int aggregate_count = rs.getInt(7);
                    final boolean is_backfill = rs.getBoolean(8);
                    final int version = rs.getInt(9);
                    final Timestamp create_time = rs.getTimestamp(10);
                    final Timestamp modify_time = rs.getTimestamp(11);
                    logger.info(
                            "id: {}, name: \"{}\", window_size: {}, event_timestamp: \"{}\", value: {}, aggregate_sum: {}, aggregate_count: {}, is_backfill: {} version: {} create_time: \"{}\" modify_time: \"{}\"",
                            id, name, window_size, event_timestamp, value, aggregate_sum, aggregate_count, is_backfill, version, create_time, modify_time
                    );
                }
            }
        }

        System.exit(exitCode);
    }
}
