package party.iroiro.r2jdbc.codecs;

import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.Type;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class DefaultCodecTest {
    @Test
    public void defaultCodecDecodeClobFails() throws SQLException, IOException {
        Clob clob = mock(Clob.class);
        InputStream thrower = mock(InputStream.class);
        when(clob.getAsciiStream()).thenReturn(thrower);
        when(thrower.readAllBytes()).thenThrow(IOException.class);

        DefaultCodec codec = new DefaultCodec();
        assertThrows(SQLException.class, () -> codec.decode(clob, String.class));
    }

    @Test
    public void defaultCodecDecodeBlobFails() throws SQLException, IOException {
        Blob clob = mock(Blob.class);
        InputStream thrower = mock(InputStream.class);
        when(clob.getBinaryStream()).thenReturn(thrower);
        when(thrower.readAllBytes()).thenThrow(IOException.class);

        DefaultCodec codec = new DefaultCodec();
        assertThrows(SQLException.class, () -> codec.decode(clob, String.class));
    }

    @Test
    public void defaultCodecConverted() {
        DefaultCodec codec = new DefaultCodec();
        assertEquals(JdbcClob.class, codec.converted(Clob.class));
        assertEquals(JdbcClob.class, codec.converted(NClob.class));
        assertEquals(JdbcBlob.class, codec.converted(Blob.class));
        assertEquals(ByteBuffer.class, codec.converted(byte[].class));
        assertEquals(Object.class, codec.converted(Object.class));
    }

    @Test
    public void defaultCodecDecodeByteArray() throws SQLException {
        DefaultCodec codec = new DefaultCodec();
        assertTrue(codec.decode(new byte[1], Object.class) instanceof ByteBuffer);
    }

    @Test
    public void defaultCodecEncodeParameter() throws SQLException {
        DefaultCodec codec = new DefaultCodec();
        assertTrue(codec.encode(mock(Connection.class), Parameters.in(new Type() {
            @Override
            public Class<?> getJavaType() {
                return Object.class;
            }

            @Override
            public String getName() {
                return "null";
            }
        }, "")) instanceof String);
    }

    @Test
    public void defaultCodecEncodeError() throws SQLException, IOException {
        DefaultCodec codec = new DefaultCodec();
        Connection connection = mock(Connection.class);
        Blob blob = mock(Blob.class);
        OutputStream stream = mock(OutputStream.class);
        doThrow(IOException.class).when(stream).close();
        when(blob.setBinaryStream(1)).thenReturn(stream);
        when(connection.createBlob()).thenReturn(blob);
        assertThrows(SQLException.class,
                () -> codec.encode(connection, new JdbcBlob(ByteBuffer.wrap(new byte[1]))));
    }
}
