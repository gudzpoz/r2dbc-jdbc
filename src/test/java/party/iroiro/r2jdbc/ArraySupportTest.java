package party.iroiro.r2jdbc;

import io.r2dbc.spi.ConnectionFactoryOptions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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

    @Test
    public void byteBufferTest() {
        String database = UUID.randomUUID().toString();
        JdbcConnectionFactory connectionFactory = new JdbcConnectionFactory(
                ConnectionFactoryOptions.parse("r2dbc:r2jdbc:h2~mem:///" + database));
        JdbcConnection conn = Objects.requireNonNull(connectionFactory.create().block());
        execute(conn, "CREATE TABLE BARR_TEST (id bigint PRIMARY KEY, arr VARBINARY)");

        execute(conn, "INSERT INTO BARR_TEST (id, arr) VALUES (?, ?)",
                1, "123".getBytes(StandardCharsets.UTF_8));
        assertInstanceOf(ByteBuffer.class, conn.createStatement("SELECT arr FROM BARR_TEST WHERE id = 1")
                .execute().flatMap(jdbcResult -> jdbcResult.map((row, rowMetadata) -> row.get(0)))
                .blockLast());

        ByteBuffer wrap = ByteBuffer.wrap("123".getBytes(StandardCharsets.UTF_8));
        assertTrue(wrap.hasArray());
        execute(conn, "INSERT INTO BARR_TEST (id, arr) VALUES (?, ?)",
                2, wrap);
        assertInstanceOf(ByteBuffer.class, conn.createStatement("SELECT arr FROM BARR_TEST WHERE id = 2")
                .execute().flatMap(jdbcResult -> jdbcResult.map((row, rowMetadata) -> row.get(0)))
                .blockLast());

        ByteBuffer direct = ByteBuffer.allocateDirect(3);
        direct.put(new byte[]{1, 2, 3});
        direct.flip();
        assertFalse(direct.hasArray());
        execute(conn, "INSERT INTO BARR_TEST (id, arr) VALUES (?, ?)",
                3, direct);
        assertInstanceOf(ByteBuffer.class, conn.createStatement("SELECT arr FROM BARR_TEST WHERE id = 3")
                .execute().flatMap(jdbcResult -> jdbcResult.map((row, rowMetadata) -> row.get(0)))
                .blockLast());
    }
}
