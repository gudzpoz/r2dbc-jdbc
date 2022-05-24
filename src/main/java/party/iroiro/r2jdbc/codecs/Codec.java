package party.iroiro.r2jdbc.codecs;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * A converter used by the JDBC worker
 *
 * <p>
 * H2, for example, does not want interruption. So implementations should avoid, at least,
 * the use of any Reactor API. If some reactive conversions are indeed necessary, do it by
 * extending {@link Converter}.
 * </p>
 */
public interface Codec extends Encoder, Decoder {
    /**
     * Guesses the JDBC type of the column
     *
     * <p>
     * The return value will be used by the JDBC worker as the second parameter of
     * {@link java.sql.ResultSet#getObject(int, Class)}.
     * </p>
     *
     * @param row    the row metadata
     * @param column column index (starts from 1, as is in JDBC)
     * @return the guessed type
     * @throws SQLException when errors occur
     */
    Class<?> guess(ResultSetMetaData row, int column) throws SQLException;

    /**
     * Returns what this implementation will encode the column type into
     *
     * @param jdbcType {@link Class} of the column data (or rather the return value of {@link #guess(ResultSetMetaData, int)}
     * @return converted Java type
     */
    Class<?> converted(Class<?> jdbcType);
}
