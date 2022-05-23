package party.iroiro.r2jdbc.codecs;

import reactor.core.publisher.Mono;

public interface Converter {
    Object convert(Object object, Class<?> target) throws UnsupportedOperationException;

    Mono<Object> convert(Object value);
}
