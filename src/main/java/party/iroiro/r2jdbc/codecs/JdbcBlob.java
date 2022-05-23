package party.iroiro.r2jdbc.codecs;

import io.r2dbc.spi.Blob;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;

@AllArgsConstructor
@Data
public class JdbcBlob implements Blob {
    private final ByteBuffer buffer;

    @Override
    public Publisher<ByteBuffer> stream() {
        return Mono.just(buffer);
    }

    @Override
    public Publisher<Void> discard() {
        return Mono.empty();
    }
}
