package party.iroiro.r2jdbc;

import io.r2dbc.spi.Nullability;
import org.junit.jupiter.api.Test;
import party.iroiro.r2jdbc.codecs.DefaultCodec;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JdbcMetadataTest {
    @Test
    public void metadataTest() throws SQLException {
        ResultSetMetaData raw = mock(ResultSetMetaData.class);
        when(raw.getColumnLabel(1)).thenReturn("label");
        when(raw.getColumnTypeName(1)).thenReturn("VARCHAR");
        when(raw.getPrecision(1)).thenReturn(40);
        when(raw.getScale(1)).thenReturn(2);
        JdbcColumnMetadata metadata = new JdbcColumnMetadata(
                raw, new DefaultCodec(), 1
        );
        assertEquals("label", metadata.getName());
        assertEquals("VARCHAR", metadata.getType().getName());
        assertEquals(40, metadata.getPrecision());
        assertEquals(2, metadata.getScale());

        assertNullability(raw, 1, ResultSetMetaData.columnNullable, Nullability.NULLABLE);
        assertNullability(raw, 1, ResultSetMetaData.columnNoNulls, Nullability.NON_NULL);
        assertNullability(raw, 1, ResultSetMetaData.columnNullableUnknown, Nullability.UNKNOWN);
        assertNullability(raw, 1, 1025, Nullability.UNKNOWN);

        JdbcRowMetadata rowMetadata = new JdbcRowMetadata(
                new ArrayList<>(Collections.singleton(metadata))
        );
        assertEquals(0, rowMetadata.getColumnIndex("label"));
        assertNotNull(rowMetadata.getColumnMetadata("label"));
        assertTrue(rowMetadata.contains("label"));
        assertFalse(rowMetadata.contains("No"));
        assertThrows(NoSuchElementException.class, () -> rowMetadata.getColumnMetadata("No"));
        assertThrows(IndexOutOfBoundsException.class, () -> rowMetadata.getColumnMetadata(99));
        assertIterableEquals(Collections.singletonList("label"), rowMetadata.getColumnNames());
        assertIterableEquals(Collections.singletonList(metadata), rowMetadata.getColumnMetadatas());
    }

    private void assertNullability(ResultSetMetaData raw, int i, int sqlNullable,
                                   Nullability nullable) throws SQLException {
        when(raw.isNullable(i)).thenReturn(sqlNullable);
        JdbcColumnMetadata metadata = new JdbcColumnMetadata(
                raw, new DefaultCodec(), i
        );
        assertEquals(nullable, metadata.getNullability());
    }

    @Test
    public void connectionMetadata() {
        JdbcConnectionMetadata metadata = new JdbcConnectionMetadata(
                mock(Connection.class), "H2", "2.1..."
        );
        assertNotNull(metadata.getConnection());
        assertEquals("H2", metadata.getDatabaseProductName());
        assertEquals("2.1...", metadata.getDatabaseVersion());
    }
}
