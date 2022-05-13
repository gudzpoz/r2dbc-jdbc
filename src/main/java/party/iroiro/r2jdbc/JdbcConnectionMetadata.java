package party.iroiro.r2jdbc;

import io.r2dbc.spi.ConnectionMetadata;
import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class JdbcConnectionMetadata implements ConnectionMetadata {
    private final String databaseProductName;
    private final String databaseVersion;
}
