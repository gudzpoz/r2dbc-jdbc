package party.iroiro.r2jdbc;

import io.r2dbc.spi.ConnectionFactoryOptions;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

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
        for (var entry : map.entrySet()) {
            String url = JdbcConnectionFactoryProvider.getJdbcConnectionUrl(
                    ConnectionFactoryOptions.parse(entry.getKey())
            ).getUrl();
            assertEquals(url, entry.getValue());
        }
    }
}
