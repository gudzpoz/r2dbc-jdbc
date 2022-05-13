package party.iroiro.r2jdbc;

import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ConnectionFactoryProvider;
import io.r2dbc.spi.NoSuchOptionException;

public class JdbcConnectionFactoryProvider implements ConnectionFactoryProvider {
    @Override
    public ConnectionFactory create(ConnectionFactoryOptions connectionFactoryOptions) {
        return new JdbcConnectionFactory(connectionFactoryOptions);
    }

    @Override
    public boolean supports(ConnectionFactoryOptions connectionFactoryOptions) {
        try {
            if (!connectionFactoryOptions.getRequiredValue(ConnectionFactoryOptions.DRIVER)
                    .equals(getDriver())) {
                return false;
            }
        } catch (NoSuchOptionException e) {
            return false;
        }
        return true;
    }

    @Override
    public String getDriver() {
        return JdbcConnectionFactoryMetadata.DRIVER_NAME;
    }
}
