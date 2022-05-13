package party.iroiro.r2jdbc;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import party.iroiro.r2jdbc.util.Pair;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.ref.Cleaner;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

@Slf4j
public class JdbcResult implements Result {
    private static final Cleaner cleaner = Cleaner.create();

    private final int[] updated;
    /**
     * Be sure NEVER access it outside the worker thread
     */
    private final AtomicReference<ResultSet> result;
    private final JdbcConnection conn;
    private final int fetchSize;
    private final ArrayList<Predicate<Segment>> filters;
    private final Exception e;

    JdbcResult(JdbcConnection conn, Object data, int fetchSize) {
        int[] finalUpdates;
        this.conn = conn;
        this.fetchSize = fetchSize;
        if (data instanceof int[]) {
            finalUpdates = (int[]) data;
        } else {
            finalUpdates = null;
        }

        if (data instanceof ResultSet) {
            this.result = new AtomicReference<>((ResultSet) data);
        } else {
            this.result = new AtomicReference<>(null);
        }

        if (data instanceof Exception) {
            this.e = (Exception) data;
        } else {
            e = null;
        }

        if (data instanceof Pair) {
            finalUpdates = (int[]) ((Pair) data).getFirst();
            this.result.set((ResultSet) ((Pair) data).getSecond());
        }

        this.updated = finalUpdates;
        filters = new ArrayList<>();
        cleaner.register(this, new ResultSetCleaner(result, conn));
    }

    public JdbcResult(JdbcConnection conn, Object data) {
        this(conn, data, -1);
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
        return conn.send(JdbcJob.Job.RESULT_METADATA, result.get(), packet -> (JdbcRowMetadata) packet.data);
    }

    private Flux<JdbcRow> fetchRows(JdbcRowMetadata metadata) {
        return Flux.create(sink -> {
            sink.onRequest(number -> {
                if (!conn.offerNow(
                        JdbcJob.Job.RESULT_ROWS,
                        new JdbcResultRequest(
                                result.get(),
                                fetchSize > 0 ? fetchSize : (int) number, metadata
                        ),
                        (packet, exception) -> {
                            List<?> list = (List<?>) packet.data;
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
                        })) {
                    sink.error(new IndexOutOfBoundsException("Unable to add fetch job to queue"));
                }
            });
            sink.onDispose(() -> {
                ResultSet set = result.getAndSet(null);
                if (!conn.offerNow(JdbcJob.Job.CLOSE_RESULT, set, ((packet, exception) -> {
                    if (exception != null) {
                        log.error("Failed to close ResultSet on disposal", exception);
                    }
                }))) {
                    log.error("Failed to offer job to dispose ResultSet");
                }
            });
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

    private static class ResultSetCleaner implements Runnable {
        private final AtomicReference<ResultSet> result;
        private final JdbcConnection conn;

        public ResultSetCleaner(AtomicReference<ResultSet> result, JdbcConnection conn) {
            this.result = result;
            this.conn = conn;
        }

        @Override
        public void run() {
            ResultSet set = result.getAndSet(null);
            if (set != null) {
                conn.offerNow(JdbcJob.Job.CLOSE_RESULT, set, ((packet, exception) -> {
                    if (exception != null) {
                        log.error("Failed to close ResultSet", exception);
                    }
                }));
            }
        }
    }
}
