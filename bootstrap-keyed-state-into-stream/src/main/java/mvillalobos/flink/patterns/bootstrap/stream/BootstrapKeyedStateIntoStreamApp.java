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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.state.api.BootstrapTransformation;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.Savepoint;
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.File;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "Boot Strap Keyed State into Stream", mixinStandardHelpOptions = true,
        description = "This demo will attempt to boot strap a dataset into a save point that will be read by a stream.")
public class BootstrapKeyedStateIntoStreamApp implements Callable<Integer> {

    private final static Logger logger = LoggerFactory.getLogger(BootstrapKeyedStateIntoStreamApp.class);

    @CommandLine.Option(names = {"-s", "--save-point-path"}, description = "The save point path.", required = true)
    private transient File savePointPath;

    public Integer call() throws Exception {
        bootstrap();
        stream();
        return 0;
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
                                        (String) row.getField(0),
                                        (String) row.getField(1),
                                        (String) row.getField(2)
                                );
                            }
                        }).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING))
                .name("bootstrap dataset");

        final BootstrapTransformation<Tuple3<String, String, String>> bootstrapTransformation = OperatorTransformation
                .bootstrapWith(bootStrapDataSet)
                .keyBy(0)
                .transform(new ConfigurationKeyedStateBootstrapFunction());

        Savepoint.create(new MemoryStateBackend(), 2)
                .withOperator("boot-strap", bootstrapTransformation)
                .write("file://" + savePointPath.getPath());
        batchEnv.execute("bootstrap demo");
    }

    //TODO while processing a stream this will use the keyed state written by bootstrap
    public void stream() {

    }

    private JDBCInputFormat buildBootStrapJdbcInputFormat(String namespace) {
        Serializable[][] queryParameters = new Serializable[1][1];
        queryParameters[0] = new Serializable[]{namespace};

        return JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
                .setDBUrl("jdbc:derby:memory:flink")
                .setQuery("SELECT namespace, name, value FROM configurations WHERE namespace = ?")
                .setRowTypeInfo(new RowTypeInfo(
                        BasicTypeInfo.STRING_TYPE_INFO, // namespace
                        BasicTypeInfo.STRING_TYPE_INFO, // name
                        BasicTypeInfo.STRING_TYPE_INFO  // value
                ))
                .setParametersProvider(new GenericParameterValuesProvider(queryParameters))
                .finish();
    }

    public static void main(String[] args) throws Exception {

        final String databaseURL = "jdbc:derby:memory:flink;create=true";
        int exitCode;
        try (final Connection con = DriverManager.getConnection(databaseURL)) {
            try (final Statement stmt = con.createStatement();) {
                stmt.execute("CREATE TABLE configurations (\n" +
                        "    id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),\n" +
                        "    namespace VARCHAR(50) NOT NULL,\n" +
                        "    name VARCHAR(50) NOT NULL,\n" +
                        "    value VARCHAR(50) NOT NULL,\n" +
                        "    version INTEGER NOT NULL DEFAULT 1,\n" +
                        "    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
                        "    modify_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
                        "    UNIQUE (name)\n" +
                        ")");
            }

            try (final PreparedStatement stmt = con.prepareStatement("INSERT INTO configurations (namespace, name, value) VALUES (?, ?, ?)")) {
                for (int i = 0; i < 10; i++) {
                    stmt.setString(1, "evil-inc");
                    stmt.setString(2, "akb-" + i);
                    stmt.setString(3, "" + i);
                    stmt.execute();
                }
            }

            exitCode = new CommandLine(new BootstrapKeyedStateIntoStreamApp()).execute(args);

            if (exitCode == 0) {
                try (final Statement stmt = con.createStatement()) {
                    final ResultSet rs = stmt.executeQuery("SELECT id, namespace, name, value, version, create_time, modify_time FROM configurations ORDER BY name");
                    while (rs.next()) {
                        final long id = rs.getLong(1);
                        final String namespace = rs.getString(2);
                        final String name = rs.getString(3);
                        final String value = rs.getString(4);
                        final int version = rs.getInt(5);
                        final Timestamp create_time = rs.getTimestamp(6);
                        final Timestamp modify_time = rs.getTimestamp(7);
                        logger.info(
                                "id: {}, namespace: \"{}\", name: \"{}\", value: {}, version: {} create_time: \"{}\" modify_time: \"{}\"",
                                id, namespace, name, value, version, create_time, modify_time
                        );
                    }
                }
            }
        }

        System.exit(exitCode);
    }

    public static class ConfigurationKeyedStateBootstrapFunction extends KeyedStateBootstrapFunction<Tuple, Tuple3<String, String, String>> {

        private transient MapState<String, Tuple3<String, String, String>> lastValues;

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String, Tuple3<String, String, String>> descriptor =
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
}
