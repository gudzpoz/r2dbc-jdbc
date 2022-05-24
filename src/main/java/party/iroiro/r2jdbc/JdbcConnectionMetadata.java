package party.iroiro.r2jdbc;

import io.r2dbc.spi.ConnectionMetadata;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class JdbcConnectionMetadata implements ConnectionMetadata {
    private final String databaseProductName;
    private final String databaseVersion;

    @Override
    public String getDatabaseProductName() {
        return databaseProductName;
    }

    @Override
    public String getDatabaseVersion() {
        return databaseVersion;
    }
}
