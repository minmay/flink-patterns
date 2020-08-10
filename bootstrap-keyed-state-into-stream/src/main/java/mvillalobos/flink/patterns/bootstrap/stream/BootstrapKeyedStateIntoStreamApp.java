package mvillalobos.flink.patterns.bootstrap.stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.split.GenericParameterValuesProvider;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.state.api.BootstrapTransformation;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.Savepoint;
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.File;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "Boot Strap Keyed State into Stream", mixinStandardHelpOptions = true,
        description = "This demo will attempt to boot strap a dataset into a save point that will be read by a stream.")
public class BootstrapKeyedStateIntoStreamApp implements Callable<Integer> {

    private final static Logger logger = LoggerFactory.getLogger(BootstrapKeyedStateIntoStreamApp.class);

    public static final String BOOT_STRAP_OPERATOR_NAME = "boot-strap";

    public enum Bootsrap {
        read,
        write;
    }

    @CommandLine.Option(names = {"-b", "--bootstrap",}, description = "Runs an embedded database.", required = true)
    private transient Bootsrap bootsrap;

    @CommandLine.Option(names = {"-s", "--save-point-path"}, description = "The save point path.")
    private transient File savePointPath;

    @CommandLine.Option(names = {"--jdbc-driver",}, description = "The jdbc password.", required = true)
    private transient String jdbcDriver;

    @CommandLine.Option(names = {"--jdbc-url"}, description = "The jdbc-url.", required = true)
    private transient String jdbcURL;

    @CommandLine.Option(names = {"--jdbc-username"}, description = "The jdbc username.", required = true)
    private transient String jdbcUsername;

    @CommandLine.Option(names = {"--jdbc-password"}, description = "The jdbc password.", required = true)
    private transient String jdbcPassword;

    public static void main(String[] args) throws Exception {
        int exitCode = new CommandLine(new BootstrapKeyedStateIntoStreamApp()).execute(args);
        System.exit(exitCode);
    }

    public Integer call() throws Exception {
        try {

            try {
                if (bootsrap == Bootsrap.write) {
                    bootstrap();
                }
            } catch (Exception e) {
                logger.error("bootstrap failed.", e);
                return -1;
            }
            if (bootsrap == Bootsrap.read) {
                stream();
            }
            return 0;
        } catch (Exception e) {
            logger.error("stream failed.", e);
            return -2;
        }
    }

    //writes dataset into a savepoint
    public void bootstrap() throws Exception {

        logger.info("Starting boot strap demo with save-point-path: file://{}", savePointPath);

        final ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        final JDBCInputFormat bootstrapJdbcInputFormat = buildBootStrapJdbcInputFormat("evil-inc");
        final DataSet<Tuple3<String, String, String>> bootStrapDataSet =
                batchEnv.createInput(bootstrapJdbcInputFormat).name("bootstrap data-source")
                        .map(new MapFunction<Row, Tuple3<String, String, String>>() {
                            @Override
                            public Tuple3<String, String, String> map(Row row) throws Exception {
                                return new Tuple3<String, String, String>(
                                        (String) row.getField(0),   // namespace
                                        (String) row.getField(1),   // name
                                        (String) row.getField(2)    // value
                                );
                            }
                        }).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING))
                .name("bootstrap dataset");

        final BootstrapTransformation<Tuple3<String, String, String>> bootstrapTransformation = OperatorTransformation
                .bootstrapWith(bootStrapDataSet)
                .keyBy(0)
                .transform(new ConfigurationKeyedStateBootstrapFunction());

        Savepoint.create(new MemoryStateBackend(), 2)
                .withOperator(BOOT_STRAP_OPERATOR_NAME, bootstrapTransformation)
                .write("file://" + savePointPath.getPath());

        batchEnv.execute("write bootstrap");
    }

    public void stream() throws Exception {
        final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        final DataStreamSource<Tuple1<String>> source = streamEnv.fromElements(new Tuple1<String>("evil-inc"));
        final DataStream<Tuple3<String, String, String>> bootstrapValues = source
                .keyBy(0)
                .process(new ReadLastValuesKeyedProcessFunction())
                .uid(BOOT_STRAP_OPERATOR_NAME)
                .name(BOOT_STRAP_OPERATOR_NAME)
                .returns(TypeInformation.of(new TypeHint<Tuple3<String, String, String>>() {}));

        bootstrapValues.print();

        streamEnv.execute("read bootstrap");
    }

    private JDBCInputFormat buildBootStrapJdbcInputFormat(String namespace) {
        Serializable[][] queryParameters = new Serializable[1][1];
        queryParameters[0] = new Serializable[]{namespace};

        return JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername(jdbcDriver)
                .setDBUrl(jdbcURL)
                .setUsername(jdbcUsername)
                .setPassword(jdbcPassword)
                .setQuery("SELECT namespace, name, value FROM configurations WHERE namespace = ?")
                .setRowTypeInfo(new RowTypeInfo(
                        BasicTypeInfo.STRING_TYPE_INFO, // namespace
                        BasicTypeInfo.STRING_TYPE_INFO, // name
                        BasicTypeInfo.STRING_TYPE_INFO  // value
                ))
                .setParametersProvider(new GenericParameterValuesProvider(queryParameters))
                .finish();
    }

    public static class ConfigurationKeyedStateBootstrapFunction extends KeyedStateBootstrapFunction<Tuple, Tuple3<String, String, String>> {

        private transient MapState<String, Tuple3<String, String, String>> lastValues;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            final MapStateDescriptor<String, Tuple3<String, String, String>> descriptor =
                    new MapStateDescriptor<String, Tuple3<String, String, String>>(
                            "bootstrap-data",
                            Types.STRING,
                            TypeInformation.of(new TypeHint<Tuple3<String, String, String>>() {
                            })
                    );

            lastValues = getRuntimeContext().getMapState(descriptor);
        }


        @Override
        public void processElement(Tuple3<String, String, String> value, Context ctx) throws Exception {
            logger.info("Adding value to map state: {}", value);
            lastValues.put(value.f1, value);
        }
    }

    public static class ReadLastValuesKeyedProcessFunction extends KeyedProcessFunction<Tuple, Tuple1<String>, Tuple3<String, String, String>> {

        private transient MapState<String, Tuple3<String, String, String>> lastValues;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            final MapStateDescriptor<String, Tuple3<String, String, String>> descriptor =
                    new MapStateDescriptor<String, Tuple3<String, String, String>>(
                            "bootstrap-data",
                            Types.STRING,
                            TypeInformation.of(new TypeHint<Tuple3<String, String, String>>() {
                            })
                    );

            lastValues = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void processElement(Tuple1<String> value, Context ctx, Collector<Tuple3<String, String, String>> out) throws Exception {
            logger.info("processing element: {}", value);

            if (lastValues.isEmpty()) {
                logger.info("no last values found.");
            } else {
                for (Map.Entry<String, Tuple3<String, String, String>> entry : lastValues.entries()) {
                    final Tuple3<String, String, String> lastValue = entry.getValue();
                    logger.info("found last value: {}", lastValue);
                    out.collect(lastValue);
                }
            }
        }
    }

}
