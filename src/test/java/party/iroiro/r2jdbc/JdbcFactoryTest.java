package party.iroiro.r2jdbc;

import io.r2dbc.spi.ConnectionFactoryOptions;
import org.junit.jupiter.api.Test;

public class JdbcFactoryTest {
    @Test
    public void nonSharedFactoryClosesNothing() {
        JdbcConnectionFactory factory = new JdbcConnectionFactory(
                ConnectionFactoryOptions.builder().build()
        );
        factory.close().block();
    }
}
