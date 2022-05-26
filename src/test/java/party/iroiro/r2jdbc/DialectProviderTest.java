package party.iroiro.r2jdbc;

import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Sort;
import org.springframework.data.r2dbc.dialect.PostgresDialect;
import org.springframework.data.r2dbc.dialect.R2dbcDialect;
import org.springframework.data.relational.core.sql.From;
import org.springframework.data.relational.core.sql.LockMode;
import org.springframework.data.relational.core.sql.LockOptions;
import org.springframework.data.relational.core.sql.SqlIdentifier;

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

    @Test
    public void customizedDialect() {
        System.setProperty("j2dialect", "org.springframework.data.r2dbc.dialect.PostgresDialect");
        JdbcConnectionFactory jdbc = new JdbcConnectionFactory(
                ConnectionFactoryOptions.builder().build()
        );
        JdbcDialectProvider provider = new JdbcDialectProvider();
        Optional<R2dbcDialect> dialect = provider.getDialect(jdbc);
        assertTrue(dialect.isPresent());
        R2dbcDialect r2dbcDialect = dialect.get();
        verifyDialect(r2dbcDialect, new PostgresDialect());
        System.setProperty("j2dialect", "");
    }

    private void verifyDialect(R2dbcDialect r2dbcDialect, PostgresDialect postgresDialect) {
        assertEquals(postgresDialect.getConverters().size(),
                r2dbcDialect.getConverters().size());
        assertEquals(Integer.class, r2dbcDialect.getArraySupport().getArrayType(int.class));
        assertTrue(r2dbcDialect.getSimpleTypeHolder().isSimpleType(Integer.class));
        assertIterableEquals(postgresDialect.getSimpleTypes(),
                r2dbcDialect.getSimpleTypes());
        assertEquals(postgresDialect.renderForGeneratedValues(SqlIdentifier.unquoted("test")),
                r2dbcDialect.renderForGeneratedValues(SqlIdentifier.unquoted("test")));
        assertNotEquals(r2dbcDialect, new Object());
        assertNotEquals(new Object().hashCode(), r2dbcDialect.hashCode());
        assertNotEquals(new Object().toString(), r2dbcDialect.toString());
        assertIterableEquals(postgresDialect.simpleTypes(), r2dbcDialect.simpleTypes());
        assertEquals(postgresDialect.getIdentifierProcessing().quote("'test'"),
                r2dbcDialect.getIdentifierProcessing().quote("'test'"));
        assertEquals(postgresDialect.getIdGeneration().driverRequiresKeyColumnNames(),
                r2dbcDialect.getIdGeneration().driverRequiresKeyColumnNames());
        assertEquals(postgresDialect.getInsertRenderContext().getDefaultValuesInsertPart(),
                r2dbcDialect.getInsertRenderContext().getDefaultValuesInsertPart());
        assertEquals(postgresDialect.getLikeEscaper().getEscapeCharacter(),
                r2dbcDialect.getLikeEscaper().getEscapeCharacter());
        assertEquals(postgresDialect.getSelectContext().evaluateOrderByNullHandling(Sort.NullHandling.NATIVE),
                r2dbcDialect.getSelectContext().evaluateOrderByNullHandling(Sort.NullHandling.NATIVE));
        assertEquals(postgresDialect.limit().getLimit(10),
                r2dbcDialect.limit().getLimit(10));
        LockOptions lockOptions = new LockOptions(LockMode.PESSIMISTIC_READ, mock(From.class));
        assertEquals(postgresDialect.lock().getLock(lockOptions),
                r2dbcDialect.lock().getLock(lockOptions));
        assertEquals(postgresDialect.orderByNullHandling().evaluate(Sort.NullHandling.NATIVE),
                r2dbcDialect.orderByNullHandling().evaluate(Sort.NullHandling.NATIVE));
    }
}
