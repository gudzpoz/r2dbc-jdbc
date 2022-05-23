package party.iroiro.r2jdbc.codecs;

import io.r2dbc.spi.Parameter;
import reactor.util.annotation.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.sql.*;
import java.time.*;
import java.util.HashMap;
import java.util.Objects;

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
        put(String.class, Types.CLOB);
        put(String.class, Types.NCLOB);
        put(ByteBuffer.class, Types.BLOB);
    }

    @Override
    public Class<?> guess(ResultSetMetaData row, int column) throws SQLException {
        return columnTypeGuesses.getOrDefault(row.getColumnType(column), null);
    }

    @Override
    public Class<?> converted(Class<?> jdbcType) {
        if (Clob.class.isAssignableFrom(jdbcType)) {
            return JdbcClob.class;
        } else if (NClob.class.isAssignableFrom(jdbcType)) {
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
            try {
                bytes = ((Clob) object).getAsciiStream().readAllBytes();
            } catch (IOException e) {
                throw new SQLException(e);
            }
            String s = new String(bytes);
            if (tClass == null || tClass.isAssignableFrom(String.class)) {
                return s;
            }
            if (tClass.isAssignableFrom(io.r2dbc.spi.Clob.class)) {
                return new JdbcClob(s);
            }
            throw new UnsupportedOperationException();
        }

        if (object instanceof Blob) {
            byte[] bytes;
            try {
                bytes = ((Blob) object).getBinaryStream().readAllBytes();
            } catch (IOException e) {
                throw new SQLException(e);
            }
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            if (tClass == null || tClass.isAssignableFrom(ByteBuffer.class)) {
                return buffer;
            }
            if (io.r2dbc.spi.Blob.class.isAssignableFrom(tClass)) {
                return new JdbcBlob(buffer);
            }
            throw new UnsupportedOperationException();
        }

        if (object instanceof byte[]) {
            return ByteBuffer.wrap((byte[]) object);
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
            try {
                OutputStream outputStream = blob.setBinaryStream(1);
                Channels.newChannel(outputStream).write(((JdbcBlob) object).getBuffer());
                outputStream.close();
            } catch (IOException e) {
                throw new SQLException(e);
            }
            return blob;
        } else {
            return object;
        }
    }
}
