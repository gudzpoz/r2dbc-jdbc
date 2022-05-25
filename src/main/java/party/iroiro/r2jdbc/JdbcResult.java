package party.iroiro.r2jdbc;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import party.iroiro.r2jdbc.codecs.Converter;
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
    private final Throwable e;
    private final Converter converter;

    JdbcResult(JdbcConnection conn, Object data, int fetchSize, Converter converter) {
        this.converter = converter;
        this.conn = conn;
        this.fetchSize = fetchSize;

        this.result = new AtomicReference<>();

        if (data instanceof ResultSet) {
            this.result.set((ResultSet) data);
        }

        if (data instanceof Throwable) {
            this.e = (Throwable) data;
        } else {
            e = null;
        }

        if (data instanceof int[]) {
            this.updated = (int[]) data;
        } else if (data instanceof Pair) {
            this.updated = (int[]) ((Pair) data).getFirst();
            this.result.set((ResultSet) ((Pair) data).getSecond());
        } else {
            this.updated = null;
        }

        filters = new ArrayList<>();
        cleaner.register(this, new ResultSetCleaner(result, conn));
    }

    public JdbcResult(JdbcConnection conn, Object data, Converter converter) {
        this(conn, data, -1, converter);
    }

    @Override
    public Flux<Integer> getRowsUpdated() {
        Flux<Integer> error = e == null ? Flux.empty() : Flux.error(e);
        if (updated == null) {
            return error;
        } else {
            return error.thenMany(Flux.fromStream(Arrays.stream(updated).boxed()));
        }
    }

    private Mono<JdbcRowMetadata> fetchMetadata() {
        return conn.send(JdbcJob.Job.RESULT_METADATA, result.get(), packet -> (JdbcRowMetadata) packet.data);
    }

    private Flux<JdbcRow> fetchRows(JdbcRowMetadata metadata) {

        if (result.get() == null) {
            return Flux.empty();
        }

        return Flux.create(sink -> {
            sink.onRequest(number -> {
                if (!conn.offerNow(
                        JdbcJob.Job.RESULT_ROWS,
                        new JdbcResultRequest(
                                result.get(),
                                fetchSize > 0 ? fetchSize : (int) number,
                                metadata
                        ),
                        (packet, exception) -> {
                            if (exception == null) {
                                List<?> list = (List<?>) packet.data;
                                for (Object item : list) {
                                    if (item == null) {
                                        sink.complete();
                                        break;
                                    }
                                    JdbcRow row = (JdbcRow) item;
                                    row.setMetadata(metadata);
                                    row.setConverter(converter);
                                    sink.next(row);
                                }
                            } else {
                                sink.error(exception);
                            }
                        })) {
                    sink.error(new IndexOutOfBoundsException("Unable to add fetch job to queue"));
                }
            });
            sink.onDispose(() -> {
                ResultSet set = result.getAndSet(null);
                if (!conn.offerNow(JdbcJob.Job.CLOSE_RESULT, set, ((packet, exception) -> {
                    if (exception != null) {
                        log.warn("Failed to close ResultSet on disposal", exception);
                    }
                }))) {
                    log.warn("Failed to offer job to dispose ResultSet");
                }
            });
        });
    }

    @Override
    public <T> Flux<T> map(BiFunction<Row, RowMetadata, ? extends T> mappingFunction) {
        return fetchMetadata().flatMapMany(metadata ->
                fetchRows(metadata).log().map(row -> mappingFunction.apply(row, metadata))
        );
    }

    @Override
    public Result filter(Predicate<Segment> filter) {
        filters.add(filter);
        return this;
    }

    @Override
    public <T> Flux<T> flatMap(Function<Segment, ? extends Publisher<? extends T>> mappingFunction) {
        return fetchMetadata().flatMapMany(metadata ->
                Flux.merge(fetchRows(metadata),
                        produceCountSegments(),
                        produceErrors()).filter(this::filter).flatMap(mappingFunction)
        );
    }

    private Mono<Segment> produceErrors() {
        if (this.e == null) {
            return Mono.empty();
        } else {
            return Mono.just(new JdbcException(this.e));
        }
    }

    private boolean filter(Segment segment) {
        return filters.stream().map(predicate -> predicate.test(segment)).reduce(true, Boolean::logicalAnd);
    }

    private Flux<Segment> produceCountSegments() {
        if (updated == null) {
            return Flux.empty();
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
