package party.iroiro.r2jdbc;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Nullability;
import io.r2dbc.spi.Type;
import lombok.AllArgsConstructor;
import lombok.Data;
import party.iroiro.r2jdbc.codecs.Codec;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

@Data
public class JdbcColumnMetadata implements ColumnMetadata {
    private final int precision;
    private final Type type;
    private final String name;
    private final int scale;
    private final int nullability;
    private final Class<?> source;

    public JdbcColumnMetadata(ResultSetMetaData metadata, Codec codec, int i) throws SQLException {
        source = codec.guess(metadata, i);
        type = new JdbcColumnMetadata.JdbcColumnType(
                codec.converted(source),
                metadata.getColumnTypeName(i)
        );
        name = metadata.getColumnName(i);
        precision = metadata.getPrecision(i);
        scale = metadata.getScale(i);
        nullability = metadata.isNullable(i);
    }

    @Override
    public Class<?> getJavaType() {
        return type.getJavaType();
    }

    @Override
    public Integer getPrecision() {
        return precision;
    }

    @Override
    public Integer getScale() {
        return scale;
    }

    public Nullability getNullability() {
        switch (nullability) {
            case ResultSetMetaData.columnNoNulls:
                return Nullability.NON_NULL;
            case ResultSetMetaData.columnNullable:
                return Nullability.NULLABLE;
            case ResultSetMetaData.columnNullableUnknown:
            default:
                return Nullability.UNKNOWN;
        }
    }

    @Override
    public Object getNativeTypeMetadata() {
        return ColumnMetadata.super.getNativeTypeMetadata();
    }

    @AllArgsConstructor
    @Data
    public static class JdbcColumnType implements Type {
        private Class<?> javaType;
        private String name;
    }
}
