package party.iroiro.r2jdbc;

import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Option;
import io.r2dbc.spi.TransactionDefinition;
import org.junit.jupiter.api.Test;
import party.iroiro.r2jdbc.codecs.DefaultConverter;
import reactor.core.publisher.Mono;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JdbcConnectionTest {
    @Test
    public void converterTest() {
        JdbcWorker worker = mock(JdbcWorker.class);
        assertThrows(JdbcException.class, () -> new JdbcConnection(worker,
                ConnectionFactoryOptions.builder()
                        .option(JdbcConnectionFactoryProvider.CONV, "org.example.NonExistent")
                        .build()
        ));

        assertThrows(JdbcException.class, () -> new JdbcConnection(worker,
                ConnectionFactoryOptions.builder()
                        .option(JdbcConnectionFactoryProvider.CONV, "java.util.concurrent.Semaphore")
                        .build()
        ));

        assertThrows(JdbcException.class, () -> new JdbcConnection(worker,
                ConnectionFactoryOptions.builder()
                        .option(JdbcConnectionFactoryProvider.CONV, "java.lang.String")
                        .build()
        ));

        assertTrue(new JdbcConnection(worker, ConnectionFactoryOptions.builder().build()).getConverter()
                instanceof DefaultConverter);

        assertTrue(new JdbcConnection(worker,
                ConnectionFactoryOptions.builder()
                        .option(JdbcConnectionFactoryProvider.CONV,
                                "party.iroiro.r2jdbc.JdbcConnectionTest$CustomConverter")
                        .build()).getConverter()
                instanceof CustomConverter);
    }


    @Test
    public void validityTest() {
        JdbcWorker worker = mock(JdbcWorker.class);
        when(worker.close()).thenReturn(Mono.empty());
        JdbcConnection connection = new JdbcConnection(worker, ConnectionFactoryOptions.builder().build());
        connection.close().block();
        assertDoesNotThrow(() ->
                connection.voidSend(JdbcJob.Job.INIT, null).block(Duration.ofSeconds(1)));
        assertDoesNotThrow(() ->
                connection.beginTransaction().block(Duration.ofSeconds(1)));
        assertDoesNotThrow(() ->
                connection.beginTransaction(new TransactionDefinition() {
                    @Override
                    public <T> T getAttribute(Option<T> option) {
                        return null;
                    }
                }).block(Duration.ofSeconds(1)));
        assertDoesNotThrow(() ->
                connection.send(JdbcJob.Job.INIT, null, ignored -> fail())
                        .block(Duration.ofSeconds(1)));
        assertDoesNotThrow(() -> connection.setTransactionIsolationLevel(IsolationLevel.READ_COMMITTED)
                .timeout(Duration.ofSeconds(1)).block());
        assertEquals(IsolationLevel.READ_COMMITTED, connection.getTransactionIsolationLevel());

        //noinspection ConstantConditions
        assertNull(connection.getMetadata());
    }

    @Test
    public void unimplemented() {
        JdbcWorker worker = mock(JdbcWorker.class);
        when(worker.close()).thenReturn(Mono.empty());
        JdbcConnection connection = new JdbcConnection(worker, ConnectionFactoryOptions.builder().build());
        connection.close().block();

        assertThrows(UnsupportedOperationException.class,
                () -> connection.createSavepoint("test"));
        assertThrows(UnsupportedOperationException.class,
                () -> connection.releaseSavepoint("test"));
        assertThrows(UnsupportedOperationException.class,
                () -> connection.rollbackTransactionToSavepoint("test"));

        assertDoesNotThrow(() -> connection.setStatementTimeout(Duration.ZERO)
                .timeout(Duration.ofSeconds(1)).block());
        assertDoesNotThrow(() -> connection.setLockWaitTimeout(Duration.ZERO)
                .timeout(Duration.ofSeconds(1)).block());
    }

    public static class CustomConverter extends DefaultConverter {
    }
}
