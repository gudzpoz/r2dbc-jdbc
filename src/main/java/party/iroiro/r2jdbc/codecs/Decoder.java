package party.iroiro.r2jdbc.codecs;

import reactor.util.annotation.Nullable;

import java.sql.SQLException;

/**
 * Decodes an object of JDBC type into the target Java type
 */
public interface Decoder {
    Object decode(Object object, @Nullable Class<?> tClass) throws UnsupportedOperationException, SQLException;
}
