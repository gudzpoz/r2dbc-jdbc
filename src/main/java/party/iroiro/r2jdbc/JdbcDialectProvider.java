package party.iroiro.r2jdbc;

import io.r2dbc.spi.ConnectionFactory;
import org.springframework.data.r2dbc.dialect.DialectResolver;
import org.springframework.data.r2dbc.dialect.H2Dialect;
import org.springframework.data.r2dbc.dialect.R2dbcDialect;
import org.springframework.r2dbc.core.binding.BindMarkersFactory;
import org.springframework.r2dbc.core.binding.BindMarkersFactoryResolver;

import java.util.Optional;

public class JdbcDialectProvider
        implements DialectResolver.R2dbcDialectProvider, BindMarkersFactoryResolver.BindMarkerFactoryProvider {
    @Override
    public Optional<R2dbcDialect> getDialect(ConnectionFactory connectionFactory) {
        if (JdbcConnectionFactoryMetadata.DRIVER_NAME.equals(
                connectionFactory.getMetadata().getName())) {
            return Optional.of(new JdbcDialect());
        } else {
            return Optional.empty();
        }
    }

    @Override
    public BindMarkersFactory getBindMarkers(ConnectionFactory connectionFactory) {
        if (JdbcConnectionFactoryMetadata.DRIVER_NAME.equals(
                connectionFactory.getMetadata().getName())) {
            return BindMarkersFactory.anonymous("?");
        } else {
            return null;
        }
    }

    private static class JdbcDialect extends H2Dialect {
        @Override
        public BindMarkersFactory getBindMarkersFactory() {
            return BindMarkersFactory.anonymous("?");
        }
    }
}
