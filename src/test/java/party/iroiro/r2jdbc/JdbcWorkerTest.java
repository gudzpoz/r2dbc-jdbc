package party.iroiro.r2jdbc;

import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.IsolationLevel;
import lbmq.LinkedBlockingMultiQueue;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import party.iroiro.lock.Lock;
import party.iroiro.lock.ReactiveLock;
import party.iroiro.r2jdbc.util.QueueDispatcher;
import reactor.test.StepVerifier;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@Slf4j
public class JdbcWorkerTest {
    private final AtomicBoolean failed = new AtomicBoolean(false);

    @Test
    public void wrongCodecTest() throws InterruptedException {
        BlockingQueue<JdbcJob> jobs = new LinkedBlockingDeque<>();
        QueueDispatcher<JdbcPacket> dispatcher = new QueueDispatcher<>(
                new LinkedBlockingMultiQueue<>()
        );
        JdbcWorker worker = new JdbcWorker(
                jobs, dispatcher.subQueue(),
                ConnectionFactoryOptions.builder()
                        .option(JdbcConnectionFactoryProvider.CODEC,
                                "party.iroiro.r2jdbc.codecs.NonExistent")
                        .build()
        );
        Thread thread = new Thread(worker);
        thread.start();
        Thread.sleep(1000);
        assertFalse(thread.isAlive());
        assertThrows(IllegalStateException.class, worker.start()::block);
    }

    @Test
    public void codecTest() throws InterruptedException {
        BlockingQueue<JdbcJob> jobs = new LinkedBlockingDeque<>();
        QueueDispatcher<JdbcPacket> dispatcher = new QueueDispatcher<>(
                new LinkedBlockingMultiQueue<>()
        );
        JdbcWorker worker = new JdbcWorker(
                jobs, dispatcher.subQueue(),
                ConnectionFactoryOptions.builder()
                        .option(JdbcConnectionFactoryProvider.CODEC,
                                "party.iroiro.r2jdbc.codecs.DefaultCodec")
                        .build()
        );
        Thread thread = new Thread(worker);
        thread.start();
        Thread.sleep(1000);
        assertTrue(thread.isAlive());
    }

    @Test
    public void workerTest() throws NoSuchFieldException, IllegalAccessException, InterruptedException, SQLException {
        BlockingQueue<JdbcJob> jobs = new LinkedBlockingDeque<>();
        QueueDispatcher<JdbcPacket> dispatcher = new QueueDispatcher<>(
                new LinkedBlockingMultiQueue<>()
        );
        JdbcWorker worker = new JdbcWorker(
                jobs, dispatcher.subQueue(),
                ConnectionFactoryOptions.builder().build()
        );
        Field field = JdbcWorker.class.getDeclaredField("conn");
        field.setAccessible(true);
        Connection connection = getMockingConnection();
        field.set(worker, connection);
        Thread dispatching = new Thread(dispatcher);
        dispatching.start();
        Thread working = new Thread(worker);
        working.start();

        Lock lock = new ReactiveLock();

        assertException(worker, lock, JdbcJob.Job.INIT, null, IllegalStateException.class);

        assertException(worker, lock, JdbcJob.Job.GET_AUTO_COMMIT, null, SQLException.class);

        assertException(worker, lock, JdbcJob.Job.SET_AUTO_COMMIT, "", IllegalArgumentException.class);
        assertException(worker, lock, JdbcJob.Job.SET_AUTO_COMMIT, true, SQLException.class);

        assertException(worker, lock, JdbcJob.Job.START_TRANSACTION, null, SQLException.class);
        assertException(worker, lock, JdbcJob.Job.END_TRANSACTION, null, SQLException.class);

        assertException(worker, lock, JdbcJob.Job.ROLLBACK_TRANSACTION, null, SQLException.class);

        assertNoException(worker, lock, JdbcJob.Job.SET_ISOLATION_LEVEL, IsolationLevel.READ_COMMITTED);
        assertNoException(worker, lock, JdbcJob.Job.SET_ISOLATION_LEVEL, IsolationLevel.READ_UNCOMMITTED);
        assertNoException(worker, lock, JdbcJob.Job.SET_ISOLATION_LEVEL, IsolationLevel.REPEATABLE_READ);
        assertException(worker, lock, JdbcJob.Job.SET_ISOLATION_LEVEL, IsolationLevel.SERIALIZABLE, SQLException.class);
        assertException(worker, lock, JdbcJob.Job.SET_ISOLATION_LEVEL, null, IllegalArgumentException.class);

        assertException(worker, lock, JdbcJob.Job.VALIDATE, null, SQLException.class);

        JdbcStatement statement = new JdbcStatement("", mock(JdbcConnection.class));
        statement.bindings.clear();
        assertException(worker, lock, JdbcJob.Job.EXECUTE_STATEMENT, statement, IllegalArgumentException.class);

        ResultSet resultSet = mock(ResultSet.class);
        doThrow(SQLException.class).when(resultSet).close();
        assertException(worker, lock, JdbcJob.Job.CLOSE_RESULT, resultSet, SQLException.class);

        lock.lock().block();
        assertFalse(failed.get());

        working.interrupt();
        dispatching.interrupt();
        working.join();
        dispatching.join();
    }

    private void assertException(JdbcWorker worker, Lock blocker,
                                 JdbcJob.Job job, Object data,
                                 Class<? extends Exception> eClass) {
        blocker.lock().block();
        JdbcWorker.offerNow(worker, job, data, ((packet, exception) -> {
            try {
                assertNotNull(exception);
                assertTrue(eClass.isAssignableFrom(exception.getClass()));
            } catch (Throwable ignored) {
                failed.set(true);
            }
            blocker.unlock();
        }));
    }

    private void assertNoException(JdbcWorker worker, Lock blocker,
                                   JdbcJob.Job job, Object data) {
        blocker.lock().block();
        JdbcWorker.offerNow(worker, job, data, ((packet, exception) -> {
            try {
                assertNull(exception);
            } catch (Throwable ignored) {
                failed.set(true);
            }
            blocker.unlock();
        }));
    }

    private Connection getMockingConnection() {
        Connection connection = mock(Connection.class);
        try {
            doThrow(SQLException.class).when(connection).getAutoCommit();
            doThrow(SQLException.class).when(connection).setAutoCommit(true);
            doThrow(SQLException.class).when(connection).setAutoCommit(false);
            doThrow(SQLException.class).when(connection).commit();
            doThrow(SQLException.class).when(connection).rollback();
            doThrow(SQLException.class).when(connection)
                    .setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            doThrow(SQLException.class).when(connection).isValid(0);
            doThrow(SQLException.class).when(connection).close();
            when(connection.prepareStatement(any())).thenReturn(mock(PreparedStatement.class));
        } catch (Throwable throwable) {
            fail();
        }
        return connection;
    }

    @Test
    public void sendTest() {
        JdbcWorker worker = mock(JdbcWorker.class);
        when(worker.notEnded()).thenReturn(false);
        JdbcWorker.voidSend(worker, JdbcJob.Job.INIT, null).as(StepVerifier::create).verifyError(
                JdbcException.class
        );
        when(worker.notEnded()).thenReturn(true);
        //noinspection unchecked
        when(worker.getJobQueue()).thenReturn(mock(BlockingQueue.class));
        when(worker.getJobQueue().offer(any())).thenAnswer(invocation -> {
            BiConsumer<JdbcPacket, Throwable> argument = invocation.getArgument(0, JdbcJob.class).consumer;
            argument.accept(null, new IllegalArgumentException());
            return true;
        });
        JdbcWorker.voidSend(worker, JdbcJob.Job.INIT, null).as(StepVerifier::create).verifyError(
                JdbcException.class
        );
    }
}
