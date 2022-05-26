package party.iroiro.r2jdbc;

import io.r2dbc.spi.ConnectionMetadata;
import lombok.AllArgsConstructor;

import java.sql.Connection;

@AllArgsConstructor
public class JdbcConnectionMetadata implements ConnectionMetadata {
    private final Connection connection;
    private final String databaseProductName;
    private final String databaseVersion;

    Connection getConnection() {
        return connection;
    }

    @Override
    public String getDatabaseProductName() {
        return databaseProductName;
    }

    @Override
    public String getDatabaseVersion() {
        return databaseVersion;
    }
}
