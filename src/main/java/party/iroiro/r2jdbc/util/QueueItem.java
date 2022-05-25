package party.iroiro.r2jdbc.util;

import lombok.AllArgsConstructor;
import reactor.core.publisher.Mono;

import java.util.function.BiConsumer;

/**
 * Conceptually just a {@link Mono}, subscribed by the {@link #consumer}
 *
 * @param <T> the published object type of this conceptual {@link Mono}
 */
@AllArgsConstructor
public class QueueItem<T> {
    public T item;
    public Exception e;
    public BiConsumer<T, Throwable> consumer;
    boolean parallel;
}
