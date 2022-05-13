package party.iroiro.r2jdbc;

import io.r2dbc.spi.ConnectionFactoryMetadata;

public class JdbcConnectionFactoryMetadata implements ConnectionFactoryMetadata {
    public static final String DRIVER_NAME = "r2jdbc";
    public static final JdbcConnectionFactoryMetadata INSTANCE = new JdbcConnectionFactoryMetadata();

    private JdbcConnectionFactoryMetadata() {}

    @Override
    public String getName() {
        return DRIVER_NAME;
    }
}
