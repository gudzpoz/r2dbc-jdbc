package party.iroiro.r2jdbc;

import io.r2dbc.spi.Result;
import org.junit.jupiter.api.Test;
import party.iroiro.r2jdbc.codecs.Converter;
import party.iroiro.r2jdbc.codecs.DefaultConverter;
import party.iroiro.r2jdbc.util.Pair;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.function.BiConsumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JdbcResultTest {
    @Test
    void resultTest() {
        Converter converter = new DefaultConverter();
        JdbcConnection connection = mock(JdbcConnection.class);
        when(connection.send(any(), any(), any())).thenReturn(Mono.just(
                new JdbcRowMetadata(new ArrayList<>())
        ));

        resultTest(converter, connection);

        updatedCountTest(converter, connection);

        exceptionTest(converter, connection);

    }

    private void resultTest(Converter converter, JdbcConnection connection) {
        ResultSet set = mock(ResultSet.class);
        JdbcResult result = new JdbcResult(
                connection, new Pair(new int[]{11}, set), converter
        );
        result.getRowsUpdated().as(StepVerifier::create).expectNext(11).verifyComplete();
        when(connection.offerNow(any(), any(), any())).thenReturn(false);
        result.map((a, b) -> 1).as(StepVerifier::create).verifyError(IndexOutOfBoundsException.class);

        result = new JdbcResult(
                connection, new Pair(new int[]{11}, set), converter
        );
        result.getRowsUpdated().as(StepVerifier::create).expectNext(11).verifyComplete();
        when(connection.offerNow(any(), any(), any())).then(invocation -> {
            //noinspection unchecked
            BiConsumer<Object, Object> argument = invocation.getArgument(2, BiConsumer.class);
            new Thread(() -> {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                }
                argument.accept(null, new IllegalAccessError());
            }).start();
            return true;
        });
        result.map((a, b) -> 1).as(StepVerifier::create).verifyError(IllegalAccessError.class);

        when(connection.offerNow(any(), any(), any())).then(invocation -> {
            //noinspection unchecked
            BiConsumer<Object, Object> argument = invocation.getArgument(2, BiConsumer.class);
            argument.accept(Collections.singletonList(null), null);
            return true;
        });
        result.map((a, b) -> 1).as(StepVerifier::create).verifyComplete();
    }

    private void exceptionTest(Converter converter, JdbcConnection connection) {
        JdbcResult result = new JdbcResult(
                connection, new IllegalAccessError(), converter
        );
        result.getRowsUpdated().as(StepVerifier::create).verifyError(IllegalAccessError.class);
    }

    private void updatedCountTest(Converter converter, JdbcConnection connection) {
        JdbcResult result;
        result = new JdbcResult(
                connection, new int[] { 11 }, converter
        );
        result.getRowsUpdated().as(StepVerifier::create).expectNext(11).verifyComplete();
        result.map((a, b) -> 1).as(StepVerifier::create).verifyComplete();
        result.flatMap((a) -> {
            if (a instanceof Result.Message) {
                return Mono.error(((Result.Message) a).exception());
            } else if (a instanceof Result.UpdateCount) {
                if (((Result.UpdateCount) a).value() == 11) {
                    return Mono.empty();
                } else {
                    return Mono.error(new Exception());
                }
            } else {
                return Mono.just(1);
            }
        }).as(StepVerifier::create).verifyComplete();
        Flux.from(result.map(r -> 1)).as(StepVerifier::create).verifyComplete();
    }
}
