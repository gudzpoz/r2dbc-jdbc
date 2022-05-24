package party.iroiro.r2jdbc.codecs;

import reactor.core.publisher.Mono;

/**
 * Conversions that happen on the reactive side
 */
public interface Converter {
    /**
     * Converts things
     *
     * <p>
     * This is used by {@link io.r2dbc.spi.Row#get(int, Class)} to loosen the requirements.
     * </p>
     *
     * @param object to be converted
     * @param target target type
     * @return converted object
     * @throws UnsupportedOperationException when not supported
     */
    Object decode(Object object, Class<?> target) throws UnsupportedOperationException;

    /**
     * Converts things
     *
     * <p>
     * This is used by {@link io.r2dbc.spi.Statement} before passing values to the worker.
     * It should rid any reactive components from {@code value} into a JDBC type or a wrapper,
     * most commonly:
     * </p>
     * <ul>
     *     <li>{@link io.r2dbc.spi.Clob} (into {@link String} or {@link JdbcClob})</li>
     *     <li>{@link io.r2dbc.spi.Blob} (into {@link java.nio.ByteBuffer} or {@link JdbcBlob}</li>
     * </ul>
     *
     * @param value the
     * @return the value to be passed to the worker
     */
    Mono<Object> encode(Object value);
}
