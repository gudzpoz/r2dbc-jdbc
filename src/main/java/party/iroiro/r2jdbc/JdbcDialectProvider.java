package party.iroiro.r2jdbc;

import io.r2dbc.spi.ConnectionFactory;
import lombok.experimental.Delegate;
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
            return Optional.of(new JdbcDialect(getDefaultDialect()));
        } else {
            return Optional.empty();
        }
    }

    private R2dbcDialect getDefaultDialect() {
        try {
            String dialect = System.getProperty("j2dialect");
            Object o = Class.forName(dialect).getConstructor().newInstance();
            return (R2dbcDialect) o;
        } catch (Throwable ignored) {
            return new H2Dialect();
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

    private static class JdbcDialect implements R2dbcDialect {

        @Delegate(types = R2dbcDialect.class, excludes = CustomizedDialect.class)
        private final R2dbcDialect defaultDialect;

        private JdbcDialect(R2dbcDialect defaultDialect) {
            this.defaultDialect = defaultDialect;
        }

        @Override
        public BindMarkersFactory getBindMarkersFactory() {
            return BindMarkersFactory.anonymous("?");
        }

        @SuppressWarnings("unused")
        private interface CustomizedDialect {
            BindMarkersFactory getBindMarkersFactory();
        }
    }
}
