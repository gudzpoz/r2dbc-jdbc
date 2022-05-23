package party.iroiro.r2jdbc.codecs;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public interface Codec extends Encoder, Decoder {
    /**
     * Guesses the JDBC type of the column
     * @param row the row metadata
     * @param column column index (starts from 1, as is in JDBC)
     * @return the guessed type
     */
    Class<?> guess(ResultSetMetaData row, int column) throws SQLException;

    Class<?> converted(Class<?> jdbcType);
}
