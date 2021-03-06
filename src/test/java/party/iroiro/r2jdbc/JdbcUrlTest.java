package party.iroiro.r2jdbc;

import io.r2dbc.spi.ConnectionFactoryOptions;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class JdbcUrlTest {
    @Test
    public void urlTest() {
        Map<String, String> map = new HashMap<>();
        map.put("r2dbc:r2jdbc:h2~tcp://~.~.localhost:8080//tmp/test",
                "jdbc:h2:tcp://localhost:8080//tmp/test");
        map.put("r2dbc:r2jdbc:h2~tcp://///localhost:8080//tmp/test",
                "jdbc:h2:tcp://localhost:8080//tmp/test");
        map.put("r2dbc:r2jdbc:h2~:////tmp/test",
                "jdbc:h2:/tmp/test");
        map.put("r2dbc:r2jdbc:oracle~thin:///~athe.oracle.db.server:8080:my_sid",
                "jdbc:oracle:thin:@the.oracle.db.server:8080:my_sid");
        map.put("r2dbc:r2jdbc:mysql~://///mysql.db.server:3306/my_database",
                "jdbc:mysql://mysql.db.server:3306/my_database");
        map.put("r2dbc:r2jdbc:some://localhost:8080",
                "jdbc:some:localhost:8080");
        map.put("r2dbc:r2jdbc:some://localhost:8080/path",
                "jdbc:some:localhost:8080/path");
        map.put("r2dbc:r2jdbc:some://localhost/path",
                "jdbc:some:localhost/path");
        for (Map.Entry<String, String> entry : map.entrySet()) {
            String url = JdbcConnectionFactoryProvider.getJdbcConnectionUrl(
                    ConnectionFactoryOptions.parse(entry.getKey())
            ).getUrl();
            assertEquals(url, entry.getValue());
        }
        assertEquals("jdbc:some::8080/path",
                JdbcConnectionFactoryProvider.getJdbcConnectionUrl(
                        ConnectionFactoryOptions.builder()
                                .option(ConnectionFactoryOptions.DRIVER, "r2jdbc")
                                .option(ConnectionFactoryOptions.PROTOCOL, "some")
                                .option(ConnectionFactoryOptions.PORT, 8080)
                                .option(ConnectionFactoryOptions.DATABASE, "path")
                                .build()
                ).getUrl());
        assertEquals("jdbc:some:host/path",
                JdbcConnectionFactoryProvider.getJdbcConnectionUrl(
                        ConnectionFactoryOptions.builder()
                                .option(ConnectionFactoryOptions.DRIVER, "r2jdbc")
                                .option(ConnectionFactoryOptions.PROTOCOL, "some")
                                .option(ConnectionFactoryOptions.HOST, "host")
                                .option(ConnectionFactoryOptions.DATABASE, "path")
                                .build()
                ).getUrl());
    }

    @Test
    public void jdbcTest() throws Exception {
        Connection connection = DriverManager.getConnection("jdbc:h2:mem:what");
        connection.prepareStatement("create table test (id integer)").execute();
        connection.prepareStatement("insert into test values (100)").execute();
        PreparedStatement preparedStatement = connection.prepareStatement("select id + 1 from test; select id - 1 from test");
        log.info(": {}", preparedStatement.execute());
        ResultSet resultSet = preparedStatement.getResultSet();
        resultSet.next();
        log.info(": {}", resultSet.getInt(1));
        log.info(": {}", resultSet.next());
        log.info(": {}", preparedStatement.getMoreResults());
    }

    @Test
    public void parameterTest() {
        String url = JdbcConnectionFactoryProvider.getJdbcConnectionUrl(
                ConnectionFactoryOptions.parse("r2dbc:r2jdbc:h2~mem:///~~~.~a~ctest")
                        .mutate()
                        .option(ConnectionFactoryOptions.USER, "sa")
                        .option(ConnectionFactoryOptions.PASSWORD, "sa")
                        .build()
        ).getUrl();
        assertEquals("jdbc:h2:mem:~/@ctest", url);
    }
}
