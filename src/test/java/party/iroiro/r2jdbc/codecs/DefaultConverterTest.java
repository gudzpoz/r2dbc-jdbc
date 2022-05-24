package party.iroiro.r2jdbc.codecs;

import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

public class DefaultConverterTest {
    @Test
    public void encodes() {
        DefaultConverter converter = new DefaultConverter();
        assertTrue(converter.encode(new JdbcClob("")).block() instanceof JdbcClob);
        assertTrue(converter.encode(Clob.from(Mono.just(""))).block() instanceof JdbcClob);
        assertTrue(converter.encode(new JdbcBlob(ByteBuffer.wrap(new byte[1]))).block() instanceof JdbcBlob);
        assertTrue(converter.encode(Blob.from(Mono.just(ByteBuffer.wrap(new byte[1])))).block() instanceof JdbcBlob);
        assertNull(converter.encode(null).block());
        assertTrue(converter.encode("").block() instanceof String);
    }

    @Test
    public void decodes() {
        DefaultConverter converter = new DefaultConverter();

        assertThrows(UnsupportedOperationException.class,
                () -> converter.decode("", Class.class));

        assertEquals("s", converter.decode("s", String.class));
        assertEquals("s", converter.decode(
                new JdbcClob("s"), String.class));

        assertTrue(converter.decode(new byte[1], ByteBuffer.class) instanceof ByteBuffer);
        assertTrue(converter.decode(ByteBuffer.allocate(1), ByteBuffer.class) instanceof ByteBuffer);
        assertTrue(converter.decode(new JdbcBlob(ByteBuffer.allocate(1)), ByteBuffer.class) instanceof ByteBuffer);
        assertThrows(UnsupportedOperationException.class,
                () -> converter.decode("No", ByteBuffer.class));

        assertTrue(converter.decode(new byte[1], Blob.class) instanceof Blob);
        assertTrue(converter.decode(ByteBuffer.allocate(1), Blob.class) instanceof Blob);
        assertTrue(converter.decode(Blob.from(Mono.empty()), Blob.class) instanceof Blob);
        assertThrows(UnsupportedOperationException.class,
                () -> converter.decode("", Blob.class));

        assertTrue(converter.decode("", Clob.class) instanceof Clob);
        assertTrue(converter.decode(Clob.from(Mono.empty()), Clob.class) instanceof Clob);
        assertThrows(UnsupportedOperationException.class,
                () -> converter.decode(ByteBuffer.allocate(1), Clob.class));
    }
}
