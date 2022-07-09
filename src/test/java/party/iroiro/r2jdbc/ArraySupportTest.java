package party.iroiro.r2jdbc;

import io.r2dbc.spi.ConnectionFactoryOptions;
import org.junit.jupiter.api.Test;

import java.util.Objects;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class ArraySupportTest {
    @Test
    public void arraySupportTest() {

        String database = UUID.randomUUID().toString();
        JdbcConnectionFactory connectionFactory = new JdbcConnectionFactory(
                ConnectionFactoryOptions.parse("r2dbc:r2jdbc:h2~mem:///" + database));
        JdbcConnection conn = Objects.requireNonNull(connectionFactory.create().block());
        execute(conn, "CREATE TABLE ARRAY_TEST (id VARCHAR, arr BIGINT ARRAY)");
        String someId = "some_id";
        execute(conn, "INSERT INTO ARRAY_TEST (id, arr) VALUES (?, ?)", someId,
                new long[]{1L, 2L, 3L, 4L, 5L});
        JdbcStatement statement = conn.createStatement("SELECT ARR FROM ARRAY_TEST WHERE id = ?");
        statement.bind(0, someId);
        Object o = statement.execute().flatMap(
                jdbcResult -> jdbcResult.map((row, rowMetadata) -> row.get(0))).blockLast();
        assertNotNull(o);
        assertTrue(o.getClass().isArray());
        assertFalse(o.getClass().getComponentType().isPrimitive());
        Object[] objects = ((Object[]) o);
        assertEquals(5, objects.length);
        for (int i = 0; i < objects.length; i++) {
            assertEquals(i + 1L, objects[i]);
        }
        conn.close().block();
        connectionFactory.close().block();
    }

    private void execute(JdbcConnection conn, String s, Object... objects) {
        JdbcStatement statement = conn.createStatement(s);
        for (int i = 0; i < objects.length; i++) {
            statement.bind(i, objects[i]);
        }
        statement.execute().blockLast();
    }
}
