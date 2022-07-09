package party.iroiro.r2jdbc.codecs;

import reactor.util.annotation.Nullable;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Encodes a Java object into a JDBC compatible type
 */
public interface Encoder {
    /**
     * From Java types into JDBC types
     *
     * @param connection the JDBC connection
     * @param object the Java object
     * @param <T> the type of Java object (probably useless)
     * @return the converted value
     * @throws UnsupportedOperationException if unsupported
     * @throws SQLException if SQL errors occur (e.g. during {@link java.sql.Clob} creation)
     */
    @Nullable
    <T> Object encode(Connection connection, T object)
            throws UnsupportedOperationException, SQLException;
}
