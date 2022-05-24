package party.iroiro.r2jdbc.codecs;

import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import org.apache.commons.beanutils.ConversionException;
import org.apache.commons.beanutils.ConvertUtilsBean;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.LinkedList;

/**
 * A default {@link Converter} using {@link ConvertUtilsBean} with custom converters to wrap {@link Clob} and {@link Blob}
 */
public class DefaultConverter implements Converter {
    protected final ConvertUtilsBean converter;

    public DefaultConverter() {
        converter = new ConvertUtilsBean();
        org.apache.commons.beanutils.Converter stringConvert = converter.lookup(String.class);
        converter.register(new org.apache.commons.beanutils.Converter() {
            @Override
            public <T> T convert(Class<T> type, Object value) {
                if (value instanceof JdbcClob) {
                    return type.cast(((JdbcClob) value).getContent());
                } else {
                    return stringConvert.convert(type, value);
                }
            }
        }, String.class);

        converter.register(new org.apache.commons.beanutils.Converter() {
            @Override
            public <T> T convert(Class<T> type, Object value) {
                if (value instanceof JdbcBlob) {
                    return type.cast(((JdbcBlob) value).getBuffer());
                } else if (value instanceof ByteBuffer) {
                    return type.cast(value);
                } else if (value instanceof byte[]) {
                    return type.cast(ByteBuffer.wrap((byte[]) value));
                } else {
                    throw new ConversionException("Unable to encode value to ByteBuffer");
                }
            }
        }, ByteBuffer.class);

        converter.register(new org.apache.commons.beanutils.Converter() {
            @Override
            public <T> T convert(Class<T> type, Object value) {
                if (value instanceof String) {
                    return type.cast(new JdbcClob((String) value));
                } else if (value instanceof Clob) {
                    return type.cast(value);
                } else {
                    throw new ConversionException("Unable to encode value to Clob");
                }
            }
        }, Clob.class);

        converter.register(new org.apache.commons.beanutils.Converter() {
            @Override
            public <T> T convert(Class<T> type, Object value) {
                if (value instanceof ByteBuffer) {
                    return type.cast(new JdbcBlob((ByteBuffer) value));
                } else if (value instanceof byte[]) {
                    return type.cast(new JdbcBlob(ByteBuffer.wrap((byte[]) value)));
                } else if (value instanceof Blob) {
                    return type.cast(value);
                } else {
                    throw new ConversionException("Unable to encode value to Blob");
                }
            }
        }, Blob.class);
    }

    @Override
    public Object decode(@Nullable Object object, Class<?> target) throws UnsupportedOperationException {
        try {
            return converter.convert(object, target);
        } catch (ConversionException e) {
            throw new UnsupportedOperationException(e);
        }
    }

    @Override
    public Mono<Object> encode(@Nullable Object value) {
        if (value instanceof Clob) {
            if (value instanceof JdbcClob) {
                return Mono.just(value);
            } else {
                return Flux.from(((Clob) value).stream()).reduce(new StringBuilder(),
                        StringBuilder::append).map(StringBuilder::toString).map(JdbcClob::new);
            }
        }

        if (value instanceof Blob) {
            if (value instanceof JdbcBlob) {
                return Mono.just(value);
            } else {
                return Flux.from(((Blob) value).stream()).reduce(new LinkedList<ByteBuffer>(),
                        (list, buffer) -> {
                            list.add(buffer);
                            return list;
                        }).map(list -> {
                    int l = list.stream().reduce(0,
                            (length, buffer) -> length + buffer.limit(), Integer::sum);
                    ByteBuffer allocate = ByteBuffer.allocate(l);
                    list.forEach(allocate::put);
                    return new JdbcBlob(allocate);
                });
            }
        }
        return Mono.justOrEmpty(value);
    }
}
