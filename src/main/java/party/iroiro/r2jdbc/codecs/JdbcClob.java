package party.iroiro.r2jdbc.codecs;

import io.r2dbc.spi.Clob;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

@AllArgsConstructor
@Data
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
}
