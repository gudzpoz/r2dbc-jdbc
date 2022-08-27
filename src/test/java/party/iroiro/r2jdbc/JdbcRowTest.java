package party.iroiro.r2jdbc;

import io.r2dbc.spi.RowMetadata;
import org.junit.jupiter.api.Test;
import party.iroiro.r2jdbc.codecs.DefaultConverter;

import java.util.ArrayList;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

public class JdbcRowTest {
    @Test
    public void jdbcRowTest() {
        JdbcRow row = new JdbcRow(new ArrayList<>(Collections.singleton(1024)));
        assertEquals(row, row.row());
        assertEquals(1024, row.get(0, Integer.class));
        assertThrows(IndexOutOfBoundsException.class, () -> row.get(1, Object.class));
        assertThrows(ClassCastException.class, () -> row.get(0, String.class));
        row.setConverter(new DefaultConverter());
        assertEquals("1024", row.get(0, String.class));

        assertThrows(NullPointerException.class, row::getMetadata);
        JdbcColumnMetadata mockColumnMetadata = mock(JdbcColumnMetadata.class);
        row.setMetadata(new JdbcRowMetadata(new ArrayList<>(Collections.singleton(mockColumnMetadata))));
        RowMetadata metadata = row.getMetadata();
        assertNotNull(metadata);
        assertSame(mockColumnMetadata, metadata.getColumnMetadata(0));
    }
}
