package party.iroiro.r2jdbc;

import io.r2dbc.spi.ConnectionFactoryOptions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/*
 * A class testing shutdown hooks correctly called.
 *
 * <p>
 * It does not seem possible to test this under JUnit though.
 * </p>
 */
public class ShutdownHookTest {
    public static void main(String[] args) {
        String database = UUID.randomUUID().toString();
        JdbcConnectionFactory connectionFactory = new JdbcConnectionFactory(
                ConnectionFactoryOptions.parse("r2dbc:r2jdbc:h2~mem:///" + database));
        JdbcConnection block = Objects.requireNonNull(connectionFactory.create().block());
        block.setAutoCommit(false).block();
        block.beginTransaction().block();
        block.createStatement("CREATE TABLE test (id bigint primary key)").execute().blockLast();
        block.commitTransaction().block();
        try {
            Thread.sleep(1000);
        } catch (Exception ignored) {
        }
    }

    @Test
    public void reflectiveShutdownTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String database = UUID.randomUUID().toString();
        JdbcConnectionFactory connectionFactory = new JdbcConnectionFactory(
                ConnectionFactoryOptions.parse("r2dbc:r2jdbc:h2~mem:///" + database));
        JdbcConnection conn = Objects.requireNonNull(connectionFactory.create().block());
        sleep();
        assertTrue(conn.getWorker().isAlive());
        sleep();
        Method shutdownMethod = JdbcWorker.class.getDeclaredMethod("shutdown");
        shutdownMethod.setAccessible(true);
        shutdownMethod.invoke(conn.getWorker());
        sleep();
        assertFalse(conn.getWorker().isAlive());
    }

    private void sleep() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
