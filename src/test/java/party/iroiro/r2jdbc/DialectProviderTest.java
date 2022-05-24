package party.iroiro.r2jdbc;

import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.junit.jupiter.api.Test;
import org.springframework.data.r2dbc.dialect.R2dbcDialect;

import java.util.Objects;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DialectProviderTest {
    @Test
    public void dialectProviderTest() {
        JdbcConnectionFactory jdbc = new JdbcConnectionFactory(ConnectionFactoryOptions.builder().build());
        JdbcDialectProvider provider = new JdbcDialectProvider();
        Optional<R2dbcDialect> dialect =
                provider.getDialect(jdbc);
        assertTrue(dialect.isPresent());
        R2dbcDialect r2dbcDialect = dialect.get();
        assertEquals("?", r2dbcDialect.getBindMarkersFactory().create().next().getPlaceholder());

        ConnectionFactory factory = mock(ConnectionFactory.class);
        ConnectionFactoryMetadata metadata = mock(ConnectionFactoryMetadata.class);
        when(factory.getMetadata()).thenReturn(metadata);
        when(metadata.getName()).thenReturn("h2");
        assertFalse(provider.getDialect(factory)
                .isPresent());

        assertNull(provider.getBindMarkers(factory));
        assertEquals("?", Objects.requireNonNull(provider.getBindMarkers(jdbc))
                .create().next().getPlaceholder());
    }
}
