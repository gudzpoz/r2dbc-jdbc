package party.iroiro.r2jdbc;

import io.r2dbc.spi.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import party.iroiro.r2jdbc.util.StringEscape;

import java.util.Properties;

public class JdbcConnectionFactoryProvider implements ConnectionFactoryProvider {
    /**
     * Converts {@link ConnectionFactoryOptions} to Jdbc urls
     *
     * <p>
     * Note that we use <code>~</code> to escape (I mean it) characters in HOST and PATH:
     * <pre><code>
     * ~~ --> ~
     * ~: --> /
     * </code></pre>
     * </p>
     * <p>
     * Several examples:
     * <pre><code>
     *         r2dbc:r2jdbc:h2~tcp://~:~:localhost:8080/test
     *           1  |   2  | 3| 4 |       5       | 6  | 7
     *         1, 2: Required
     *         3: Database Driver
     *         4: Protocol
     *         5: Host (Note that ~:~: will be encoded into //)
     *         6: Port
     *         7: Path
     *
     *         Then the encoded url:
     *         jdbc:{$3}:{$4}:{$5}:{$6}/{$7}
     *         jdbc:h2:tcp://localhost:8080/test
     *         </code></pre>
     * </p>
     * <p>
     * Some more examples:
     * <pre><code>
     *   map.put("r2dbc:r2jdbc:h2~tcp://~.~.localhost:8080//tmp/test",
     *           "jdbc:h2:tcp://localhost:8080//tmp/test");
     *   map.put("r2dbc:r2jdbc:h2~tcp://///localhost:8080//tmp/test",
     *           "jdbc:h2:tcp://localhost:8080//tmp/test");
     *   map.put("r2dbc:r2jdbc:h2~:////tmp/test",
     *           "jdbc:h2:/tmp/test");
     *   map.put("r2dbc:r2jdbc:oracle~thin:///~athe.oracle.db.server:8080:my_sid",
     *           "jdbc:oracle:thin:@the.oracle.db.server:8080:my_sid");
     *   map.put("r2dbc:r2jdbc:mysql~://///mysql.db.server:3306/my_database",
     *           "jdbc:mysql://mysql.db.server:3306/my_database");
     *     </code></pre>
     * </p>
     *
     * @param options the input options
     * @return somehow corresponding jdbc url
     * @throws NoSuchOptionException when DRIVER and PROTOCOL not found
     */
    public static JdbcConnectionDetails getJdbcConnectionUrl(ConnectionFactoryOptions options) throws NoSuchOptionException {
        String driver = options.getRequiredValue(ConnectionFactoryOptions.DRIVER).toString();
        if (!driver.equals(JdbcConnectionFactoryMetadata.DRIVER_NAME)) {
            throw new IllegalArgumentException(ConnectionFactoryOptions.DRIVER.toString());
        }

        String user = (String) options.getValue(ConnectionFactoryOptions.USER);
        String password = (String) options.getValue(ConnectionFactoryOptions.PASSWORD);
        String forwardString = (String) options.getValue(Option.valueOf("j2forward"));
        String[] forwarded = forwardString == null ? new String[0] : forwardString.split(",");
        Properties properties = new Properties();
        if (user != null) {
            properties.put("user", user);
        }
        if (password != null) {
            properties.put("password", user);
        }
        for (String option : forwarded) {
            properties.put(option, options.getValue(Option.valueOf(option)));
        }

        String forced = (String) options.getValue(Option.valueOf("j2path"));
        if (forced != null) {
            return new JdbcConnectionDetails(properties, forced);
        }

        StringEscape escape = new StringEscape('~', character -> {
            switch (character) {
                case '.':
                    return '/';
                case '~':
                    return '~';
                case 'a':
                    return '@';
                default:
                    return character;
            }
        });
        String protocol = options.getRequiredValue(ConnectionFactoryOptions.PROTOCOL).toString();
        String host = (String) options.getValue(ConnectionFactoryOptions.HOST);
        Integer port = (Integer) options.getValue(ConnectionFactoryOptions.PORT);
        String path = (String) options.getValue(ConnectionFactoryOptions.DATABASE);

        String[] dbProtocol = protocol.split("~", 2);
        String databaseType = dbProtocol[0];

        StringBuilder builder = new StringBuilder("jdbc:");
        builder.append(databaseType).append(':');
        if (dbProtocol.length == 2 && !dbProtocol[1].equals("")) {
            builder.append(dbProtocol[1]).append(':');
        }
        if (host != null) {
            builder.append(escape.unescape(host));
        }
        if (port != null) {
            builder.append(':').append(port);
        }
        if (path != null) {
            if (host != null || port != null) {
                builder.append('/');
            }
            builder.append(escape.unescape(path));
        }
        return new JdbcConnectionDetails(properties, builder.toString());
    }

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

    @AllArgsConstructor
    @Data
    public static class JdbcConnectionDetails {
        private final Properties properties;
        private final String url;
    }
}
