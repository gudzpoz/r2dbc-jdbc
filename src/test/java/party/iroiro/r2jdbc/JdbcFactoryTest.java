package party.iroiro.r2jdbc;

import io.r2dbc.spi.ConnectionFactoryOptions;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

public class JdbcFactoryTest {
    @Test
    public void nonSharedFactoryClosesNothing() {
        JdbcConnectionFactory factory = new JdbcConnectionFactory(
                ConnectionFactoryOptions.builder().build()
        );
        factory.close().as(StepVerifier::create).verifyComplete();
    }
}
