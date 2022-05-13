package party.iroiro.r2jdbc;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

@Slf4j
public class JdbcResult implements Result {
    private final int[] updated;
    /**
     * Be sure NEVER access it outside the worker thread
     */
    private final ResultSet result;
    private final JdbcConnection conn;
    private final ArrayList<Predicate<Segment>> filters;
    private final Exception e;

    public JdbcResult(JdbcConnection conn, Object data) {
        this.conn = conn;
        if (data instanceof int[]) {
            this.updated = (int[]) data;
        } else {
            this.updated = null;
        }

        if (data instanceof ResultSet) {
            this.result = (ResultSet) data;
        } else {
            this.result = null;
        }

        if (data instanceof Exception) {
            this.e = (Exception) data;
        } else {
            e = null;
        }
        filters = new ArrayList<>();
    }

    @Override
    public Publisher<Long> getRowsUpdated() {
        if (updated == null) {
            return Flux.empty();
        } else {
            return Flux.fromStream(Arrays.stream(updated).asLongStream().boxed());
        }
    }

    private Mono<JdbcRowMetadata> fetchMetadata() {
        return conn.send(JdbcJob.Job.RESULT_METADATA, result, packet -> (JdbcRowMetadata) packet.data);
    }

    private Flux<JdbcRow> fetchRows(JdbcRowMetadata metadata) {
        return Flux.create(sink -> {
            sink.onRequest(number -> conn.send(JdbcJob.Job.RESULT_ROWS,
                    new JdbcResultRequest(result, (int) number, metadata),
                    packet -> (List<?>) packet.data)
                    .doOnNext(list -> {
                        for (Object item : list) {
                            if (item == null) {
                                sink.complete();
                                break;
                            }
                            assert item instanceof JdbcRow;
                            JdbcRow row = (JdbcRow) item;
                            row.setMetadata(metadata);
                            sink.next(row);
                        }
                    }).doOnError(sink::error)
                    .subscribe());
            sink.onDispose(() -> conn.voidSend(JdbcJob.Job.CLOSE_RESULT, result).subscribe());
        });
    }

    @Override
    public <T> Publisher<T> map(BiFunction<Row, RowMetadata, ? extends T> mappingFunction) {
        return fetchMetadata().flatMapMany(metadata ->
                fetchRows(metadata).map(row -> mappingFunction.apply(row, metadata))
        );
    }

    @Override
    public Result filter(Predicate<Segment> filter) {
        filters.add(filter);
        return this;
    }

    @Override
    public <T> Publisher<T> flatMap(Function<Segment, ? extends Publisher<? extends T>> mappingFunction) {
        return fetchMetadata().flatMapMany(metadata ->
                Flux.merge(fetchRows(metadata),
                        produceCountSegments(),
                        produceErrors()).filter(this::filter).flatMap(mappingFunction)
        );
    }

    private Publisher<Segment> produceErrors() {
        if (this.e == null) {
            return Mono.empty();
        } else {
            return Mono.just(new JdbcException(this.e));
        }
    }

    private boolean filter(Segment segment) {
        return filters.stream().map(predicate -> predicate.test(segment)).reduce(true, Boolean::logicalAnd);
    }

    private Publisher<Segment> produceCountSegments() {
        if (updated == null) {
            return Mono.empty();
        } else {
            return Flux.fromStream(Arrays.stream(updated).boxed()).map(count -> {
                if (count >= 0) {
                    return (UpdateCount) () -> count;
                } else {
                    return new JdbcException(count);
                }
            });
        }
    }

    @AllArgsConstructor
    @Getter
    public static class JdbcResultRequest {
        final ResultSet result;
        final int count;
        final JdbcRowMetadata columns;
    }
}
