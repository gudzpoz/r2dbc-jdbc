package party.iroiro.r2jdbc.codecs;

import io.r2dbc.spi.Parameter;
import reactor.util.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.sql.*;
import java.time.*;
import java.util.HashMap;
import java.util.Objects;

/**
 * A default {@link Codec} with {@link Blob} and {@link Clob} conversions
 */
public class DefaultCodec implements Codec {
    private final HashMap<Integer, Class<?>> columnTypeGuesses;

    public DefaultCodec() {
        columnTypeGuesses = new HashMap<>();

        initGuessMap();
    }

    private void initGuessMap() {
        put(Object.class, Types.NULL);
        put(String.class, Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR,
                Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR);
        put(BigDecimal.class, Types.NUMERIC, Types.DECIMAL);
        put(Long.class, Types.BIGINT);
        put(Integer.class, Types.INTEGER);
        put(Short.class, Types.SMALLINT);
        put(Byte.class, Types.TINYINT);
        put(Float.class, Types.FLOAT, Types.REAL);
        put(Double.class, Types.DOUBLE);
        put(byte[].class, Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY);
        put(Boolean.class, Types.BOOLEAN, Types.BIT);
        put(LocalDate.class, Types.DATE);
        put(LocalTime.class, Types.TIME);
        put(OffsetTime.class, Types.TIME_WITH_TIMEZONE);
        put(LocalDateTime.class, Types.TIMESTAMP);
        put(OffsetDateTime.class, Types.TIMESTAMP_WITH_TIMEZONE);
        put(Object.class, Types.JAVA_OBJECT, Types.OTHER);
        put(Object[].class, Types.ARRAY);
        put(Clob.class, Types.CLOB);
        put(Clob.class, Types.NCLOB);
        put(Blob.class, Types.BLOB);
    }

    @Override
    public Class<?> guess(ResultSetMetaData row, int column) throws SQLException {
        return columnTypeGuesses.getOrDefault(row.getColumnType(column), null);
    }

    @Override
    public Class<?> converted(Class<?> jdbcType) {
        if (Clob.class.isAssignableFrom(jdbcType)) {
            return JdbcClob.class;
        } else if (Blob.class.isAssignableFrom(jdbcType)) {
            return JdbcBlob.class;
        } else if (byte[].class.equals(jdbcType)) {
            return ByteBuffer.class;
        } else {
            return jdbcType;
        }
    }

    private void put(Class<?> clazz, int... types) {
        for (int type : types) {
            columnTypeGuesses.put(type, clazz);
        }
    }

    @Override
    public Object decode(Object object, @Nullable Class<?> tClass)
            throws UnsupportedOperationException, SQLException {
        if (object instanceof Clob) {
            byte[] bytes;
            try (InputStream asciiStream = ((Clob) object).getAsciiStream()) {
                bytes = asciiStream.readAllBytes();
            } catch (IOException e) {
                throw new SQLException(e);
            }
            return new String(bytes);
        }

        if (object instanceof Blob) {
            byte[] bytes;
            try (InputStream binaryStream = ((Blob) object).getBinaryStream()) {
                bytes = binaryStream.readAllBytes();
            } catch (IOException e) {
                throw new SQLException(e);
            }
            return ByteBuffer.wrap(bytes);
        }

        if (object instanceof byte[]) {
            return ByteBuffer.wrap((byte[]) object);
        }

        if (object instanceof Array) {
            Object array = ((Array) object).getArray();
            ((Array) object).free();
            return array;
        }

        return object;
    }

    @Override
    @Nullable
    public <T> Object encode(Connection connection, T o)
            throws UnsupportedOperationException, SQLException {
        Object object = Objects.requireNonNull(o);
        if (object instanceof Parameter) {
            Object parameter = ((Parameter) object).getValue();
            if (parameter == null) {
                return null;
            } else {
                return encode(connection, parameter);
            }
        } else if (object instanceof JdbcClob) {
            Clob clob = connection.createClob();
            clob.setString(1, ((JdbcClob) object).getContent());
            return clob;
        } else if (object instanceof JdbcBlob) {
            Blob blob = connection.createBlob();
            try (OutputStream outputStream = blob.setBinaryStream(1)) {
                Channels.newChannel(outputStream).write(((JdbcBlob) object).getBuffer());
            } catch (IOException e) {
                throw new SQLException(e);
            }
            return blob;
        } else if (object instanceof byte[]){
            return object;
        } else if (object instanceof ByteBuffer) {
            ByteBuffer buffer = (ByteBuffer) object;
            if (buffer.hasArray()) {
                return buffer.array();
            } else {
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
                return bytes;
            }
        } else {
            Class<?> aClass = object.getClass();
            if (aClass.isArray() && aClass.getComponentType().isPrimitive()) {
                return boxedArray(object);
            }
            return object;
        }
    }

    protected Object[] boxedArray(Object object) {
        int length = java.lang.reflect.Array.getLength(object);
        Object[] objects = new Object[length];
        for (int i = 0; i < length; i++) {
            objects[i] = java.lang.reflect.Array.get(object, i);
        }
        return objects;
    }
}
