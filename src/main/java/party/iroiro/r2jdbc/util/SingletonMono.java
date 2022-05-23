package party.iroiro.r2jdbc.util;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

public class SingletonMono<T> {
    private final Sinks.Many<T> sink;
    private final Flux<T> flux;

    public SingletonMono() {
        sink = Sinks.many().replay().latest();
        flux = sink.asFlux();
    }

    public void set(T t) {
        if (t != null) {
            sink.tryEmitNext(t);
        }
    }

    public Mono<T> get() {
        return flux.next();
    }
}
