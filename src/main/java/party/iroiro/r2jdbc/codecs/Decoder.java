package party.iroiro.r2jdbc.codecs;

import reactor.util.annotation.Nullable;

import java.sql.SQLException;

/**
 * Decodes an object of JDBC type into the target Java type
 */
public interface Decoder {
    /**
     * From JDBC types to common Java types
     *
     * @param object the JDBC value
     * @param tClass target type
     * @return converted value
     * @throws UnsupportedOperationException if conversion unsupported
     * @throws SQLException if SQL errors occur (e.g. during {@link java.sql.Clob} conversions)
     */
    Object decode(Object object, @Nullable Class<?> tClass) throws UnsupportedOperationException, SQLException;
}
