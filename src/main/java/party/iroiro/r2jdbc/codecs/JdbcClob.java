package party.iroiro.r2jdbc.codecs;

import io.r2dbc.spi.Clob;
import lombok.AllArgsConstructor;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

/**
 * Thin wrapper around {@link String} to serve a {@link Clob} interface
 */
@AllArgsConstructor
public class JdbcClob implements Clob {
    private final String content;

    @Override
    public Publisher<CharSequence> stream() {
        return Mono.just(content);
    }

    @Override
    public Publisher<Void> discard() {
        return Mono.empty();
    }

    public String getContent() {
        return content;
    }
}
