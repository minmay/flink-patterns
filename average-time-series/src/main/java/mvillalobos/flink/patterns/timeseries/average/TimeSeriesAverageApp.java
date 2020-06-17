package mvillalobos.flink.patterns.timeseries.average;

import com.google.common.collect.ImmutableList;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.jdbc.JDBCOptions;
import org.apache.flink.api.java.io.jdbc.JDBCUpsertTableSink;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;

public class TimeSeriesAverageApp {

    private final static Logger logger = LoggerFactory.getLogger(TimeSeriesAverageApp.class);

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
        // f6: is_backfile: boolean
        // THEN the stream is KEYED BY: f0: name:String, f1: window_size: int
        // THEN the stream is WINDOWED into a tumbling event time window of 15 minutes
        // THEN the window is configured to allow elements late by 1 hour
        // THEN a low-level process function is applied to the window that
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

                        out.collect(aggregation);
                    }
                });

        upsertToJDBC(jdbcUpsertTableSink, aggregateTimeSeriesStream);

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
        }));
    }

    public static void main(String[] args) throws Exception {
        logger.info("Command line arguments: {}", Arrays.toString(args));
        final String inputFilePath = args[0];
        logger.info("Reading input file: {}", inputFilePath);

        final String databaseURL = "jdbc:derby:memory:flink;create=true";
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

            TimeSeriesAverageApp app = new TimeSeriesAverageApp();
            app.stream(inputFilePath);

            try (final Statement stmt = con.createStatement()) {
                final ResultSet rs = stmt.executeQuery("SELECT id, name, window_size, event_timestamp, value, aggregate_sum, aggregate_count, is_backfill, version, create_time, modify_time FROM time_series");
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
    }
}
