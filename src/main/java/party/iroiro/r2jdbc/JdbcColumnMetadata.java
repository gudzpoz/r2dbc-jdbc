package party.iroiro.r2jdbc;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Type;
import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class JdbcColumnMetadata implements ColumnMetadata {
    @AllArgsConstructor
    @Data
    public static class JdbcColumnType implements Type {
        private Class<?> javaType;
        private String name;
    }
    private Type type;
    private String name;
}
