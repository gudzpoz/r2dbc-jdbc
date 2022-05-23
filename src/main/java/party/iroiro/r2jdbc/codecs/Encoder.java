package party.iroiro.r2jdbc.codecs;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Encodes a Java object into a JDBC compatible type
 */
public interface Encoder {
    <T> Object encode(Connection connection, T object)
            throws UnsupportedOperationException, SQLException;
}
