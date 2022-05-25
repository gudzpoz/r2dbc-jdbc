package party.iroiro.r2jdbc;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.IsolationLevel;
import lbmq.LinkedBlockingMultiQueue;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.ConstructorUtils;
import party.iroiro.r2jdbc.codecs.Codec;
import party.iroiro.r2jdbc.codecs.DefaultCodec;
import party.iroiro.r2jdbc.util.Pair;
import party.iroiro.r2jdbc.util.QueueItem;
import party.iroiro.r2jdbc.util.SingletonMono;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;

@Slf4j
class JdbcWorker implements Runnable {

    private final BlockingQueue<JdbcJob> jobs;
    private final LinkedBlockingMultiQueue<Integer, QueueItem<JdbcPacket>>.SubQueue out;
    private final ConnectionFactoryOptions options;
    private final Thread thread;
    private final SingletonMono<JdbcConnectionMetadata> metadata;
    private final AtomicInteger refCount;
    private final Duration pollDuration;
    private final boolean shared;
    private final List<JdbcJob> closeJobs;
    private State state;
    private Codec codec;
    private Connection conn;

    JdbcWorker(BlockingQueue<JdbcJob> jobs,
               LinkedBlockingMultiQueue<Integer, QueueItem<JdbcPacket>>.SubQueue out,
               ConnectionFactoryOptions options) {
        this.jobs = jobs;
        this.out = out;
        this.options = options;
        this.conn = null;
        this.thread = new Thread(this);
        String waitTime = (String) options.getValue(JdbcConnectionFactoryProvider.WAIT);
        int time = waitTime == null ? 0 : Integer.parseInt(waitTime);
        this.pollDuration = Duration.ofMillis(time);
        metadata = new SingletonMono<>();
        refCount = new AtomicInteger(0);
        shared = options.hasOption(JdbcConnectionFactoryProvider.SHARED);
        codec = null;
        closeJobs = new LinkedList<>();
        state = State.RUNNABLE;
    }

    private static Connection getConnection(ConnectionFactoryOptions options) throws SQLException {
        JdbcConnectionFactoryProvider.JdbcConnectionDetails details =
                JdbcConnectionFactoryProvider.getJdbcConnectionUrl(options);
        return DriverManager.getConnection(details.getUrl(), details.getProperties());
    }

    static Mono<Void> voidSend(JdbcWorker worker, JdbcJob.Job job, @Nullable Object data) {
        return Mono.create(voidMonoSink -> voidMonoSink.onRequest(
                ignored -> {
                    if (!offerNow(worker, job, data, (i, e) -> {
                        if (e == null) {
                            voidMonoSink.success();
                        } else {
                            voidMonoSink.error(new JdbcException(e));
                        }
                    })) {
                        voidMonoSink.error(new JdbcException(new IndexOutOfBoundsException("Unable to push to queue")));
                    }
                }));
    }

    static boolean offerNow(JdbcWorker worker,
                            JdbcJob.Job job,
                            @Nullable Object data,
                            BiConsumer<JdbcPacket, Throwable> consumer) {
        if (worker.notEnded()) {
            // FIXME: Edge cases
            return worker.getJobQueue().offer(new JdbcJob(job, data, consumer));
        } else {
            return false;
        }
    }

    static <T> Mono<T> send(JdbcWorker worker, JdbcJob.Job job,
                            @Nullable Object data, Function<JdbcPacket, T> converter) {
        return Mono.create(sink -> sink.onRequest(
                ignored -> {
                    if (!offerNow(worker, job, data, (i, e) -> {
                        if (e == null) {
                            sink.success(converter.apply(i));
                        } else {
                            sink.error(new JdbcException(e));
                        }
                    })) {
                        sink.error(new JdbcException(new IndexOutOfBoundsException("Unable to push to queue")));
                    }
                }));
    }

    private synchronized boolean notEnded() {
        return state != State.ENDED;
    }

    private Codec initCodec()
            throws ClassNotFoundException, ClassCastException, InvocationTargetException,
            NoSuchMethodException, IllegalAccessException, InstantiationException {
        if (options.hasOption(JdbcConnectionFactoryProvider.CODEC)) {
            String value = (String) options.getValue(JdbcConnectionFactoryProvider.CODEC);
            Class<?> aClass = Class.forName(value);
            if (Codec.class.isAssignableFrom(aClass)) {
                return (Codec) ConstructorUtils.invokeConstructor(aClass, null);
            } else {
                throw new ClassCastException(aClass.getName());
            }
        } else {
            return new DefaultCodec();
        }
    }

    private void offer(JdbcPacket packet, BiConsumer<JdbcPacket, Throwable> consumer) {
        out.offer(new QueueItem<>(packet, null, consumer, true));
    }

    private void offer(BiConsumer<JdbcPacket, Throwable> consumer) {
        out.offer(new QueueItem<>(null, null, consumer, false));
    }

    private void offer(Exception e, BiConsumer<JdbcPacket, Throwable> consumer) {
        out.offer(new QueueItem<>(
                null, e, consumer, true
        ));
    }

    private void takeAndProcess() throws InterruptedException {
        JdbcJob job = (pollDuration.isZero() || pollDuration.isNegative()) ?
                jobs.take() : jobs.poll(pollDuration.toMillis(), TimeUnit.MILLISECONDS);
        if (job == null) {
            synchronized (this) {
                if (refCount.get() == 0) {
                    if (shared) {
                        state = State.ENDED;
                        throw new InterruptedException("Closing after no connection for sometime");
                    }
                }
            }
            return;
        }
        try {
            process(job);
        } catch (InterruptedException e) {
            throw e;
        } catch (Throwable any) {
            log.error("Unexpected exception", any);
            offer(new JdbcException(any), job.consumer);
        }
    }

    private void process(JdbcJob job) throws InterruptedException {
        log.trace("Processing: {}", job.job);
        switch (job.job) {
            case INIT:
                if (conn != null) {
                    offer(new IllegalStateException("Tries to initialize twice"), job.consumer);
                } else {
                    try {
                        conn = getConnection(options);
                        DatabaseMetaData metaData = conn.getMetaData();
                        offer(new JdbcPacket(
                                new JdbcConnectionMetadata(
                                        metaData.getDatabaseProductName(),
                                        metaData.getDatabaseProductVersion()
                                )
                        ), job.consumer);
                    } catch (SQLException e) {
                        offer(e, job.consumer);
                    }
                }
                break;
            case GET_AUTO_COMMIT:
                try {
                    boolean autoCommit = conn.getAutoCommit();
                    offer(new JdbcPacket(autoCommit), job.consumer);
                } catch (SQLException e) {
                    offer(e, job.consumer);
                }
                break;
            case SET_AUTO_COMMIT:
                if (job.data instanceof Boolean) {
                    try {
                        conn.setAutoCommit((Boolean) job.data);
                        offer(job.consumer);
                    } catch (SQLException e) {
                        offer(e, job.consumer);
                    }
                } else {
                    offer(new IllegalArgumentException("Expected Boolean data"), job.consumer);
                }
                break;
            case START_TRANSACTION:
                try {
                    conn.setAutoCommit(false);
                    offer(job.consumer);
                } catch (SQLException e) {
                    offer(e, job.consumer);
                }
                break;
            case END_TRANSACTION:
                try {
                    conn.commit();
                    offer(job.consumer);
                } catch (SQLException e) {
                    offer(e, job.consumer);
                }
                break;
            case ROLLBACK_TRANSACTION:
                try {
                    conn.rollback();
                    offer(job.consumer);
                } catch (SQLException e) {
                    offer(e, job.consumer);
                }
                break;
            case SET_ISOLATION_LEVEL:
                try {
                    if (IsolationLevel.REPEATABLE_READ.equals(job.data)) {
                        conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
                    } else if (IsolationLevel.SERIALIZABLE.equals(job.data)) {
                        conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
                    } else if (IsolationLevel.READ_UNCOMMITTED.equals(job.data)) {
                        conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                    } else if (IsolationLevel.READ_COMMITTED.equals(job.data)) {
                        conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                    } else {
                        offer(new IllegalArgumentException("Unrecognized isolation level"), job.consumer);
                        break;
                    }
                    offer(job.consumer);
                } catch (SQLException e) {
                    offer(e, job.consumer);
                }
                break;
            case VALIDATE:
                try {
                    boolean validity = conn.isValid(0);
                    offer(new JdbcPacket(validity), job.consumer);
                } catch (SQLException e) {
                    offer(e, job.consumer);
                }
                break;
            case EXECUTE_STATEMENT:
                JdbcStatement statement = (JdbcStatement) job.data;
                try {
                    Object result = execute(statement.sql, statement.bindings, statement.wantsGenerated.get());
                    offer(new JdbcPacket(result), job.consumer);
                } catch (SQLException e) {
                    offer(e, job.consumer);
                }
                break;
            case BATCH:
                JdbcBatch batch = (JdbcBatch) job.data;
                List<Object> results = new ArrayList<>(batch.sql.size());
                for (String s : batch.sql) {
                    try {
                        Object result = execute(s, null, null);
                        results.add(result);
                    } catch (SQLException e) {
                        results.add(e);
                    }
                }
                offer(new JdbcPacket(results), job.consumer);
                break;
            case RESULT_METADATA:
                ResultSet result = (ResultSet) job.data;
                if (result == null) {
                    offer(new JdbcException(new IllegalArgumentException("Null ResultSet")), job.consumer);
                    break;
                }
                try {
                    ResultSetMetaData metadata = result.getMetaData();
                    int count = metadata.getColumnCount();
                    ArrayList<JdbcColumnMetadata> columns = new ArrayList<>(count);
                    for (int i = 0; i < count; i++) {
                        log.trace("Column Type: {}", metadata.getColumnType(i + 1));
                        columns.add(new JdbcColumnMetadata(metadata, codec, i + 1));
                    }
                    offer(new JdbcPacket(new JdbcRowMetadata(columns)), job.consumer);
                } catch (SQLException e) {
                    offer(e, job.consumer);
                }
                break;
            case RESULT_ROWS:
                JdbcResult.JdbcResultRequest request = (JdbcResult.JdbcResultRequest) job.data;
                if (request.result == null) {
                    offer(new JdbcException(new IllegalArgumentException("Null ResultSet")), job.consumer);
                    break;
                }
                ArrayList<JdbcRow> response = new ArrayList<>(request.count + 1);
                try {
                    ResultSet row = request.result;
                    row.setFetchSize(Math.max(request.count, 0));
                    int limit = request.count > 0 ? request.count : Integer.MAX_VALUE;
                    List<? extends ColumnMetadata> metadata = request.columns.getColumnMetadatas();
                    int columns = metadata.size();
                    for (int i = 0; i < limit; i++) {
                        if (!row.next()) {
                            response.add(null);
                            break;
                        }
                        ArrayList<Object> rowData = new ArrayList<>(columns);
                        for (int j = 0; j < columns; j++) {
                            ColumnMetadata meta = metadata.get(j);
                            Class<?> type = (Class<?>) meta.getNativeTypeMetadata();
                            if (type == null) {
                                rowData.add(codec.decode(row.getObject(j + 1), meta.getJavaType()));
                            } else {
                                rowData.add(codec.decode(row.getObject(j + 1, type), meta.getJavaType()));
                            }
                        }
                        response.add(new JdbcRow(rowData));
                    }
                    offer(new JdbcPacket(response), job.consumer);
                } catch (SQLException e) {
                    offer(e, job.consumer);
                }
                break;
            case CLOSE_RESULT:
                ResultSet closable = (ResultSet) job.data;
                try {
                    closable.close();
                    offer(job.consumer);
                } catch (SQLException e) {
                    offer(e, job.consumer);
                }
                break;
            case CLOSE:
                synchronized (this) {
                    state = State.ENDED;
                }
                closeJobs.add(job);
                throw new InterruptedException("Connection closing");
        }
        log.trace("Process finished: {}", job.job);
    }

    private Object execute(String sql, @Nullable ArrayList<Map<Integer, Object>> bindings,
                           @Nullable String[] keys) throws SQLException {
        PreparedStatement s = getCachedOrPrepare(sql, keys);
        ArrayList<Object> results = new ArrayList<>(bindings == null ? 1 : bindings.size());
        if (bindings == null) {
            executeSingle(results, s, null);
        } else if (bindings.size() == 0) {
            s.close();
            throw new IllegalArgumentException("No valid statement");
        } else {
            for (Map<Integer, Object> binding : bindings) {
                executeSingle(results, s, binding);
            }
        }
        return results;
    }

    private void executeSingle(ArrayList<Object> results, PreparedStatement s,
                               @Nullable Map<Integer, Object> binding) throws SQLException {
        if (binding != null) {
            bindStatement(s, binding);
        }
        boolean isQuery = s.execute();
        if (isQuery) {
            do {
                ResultSet resultSet = s.getResultSet();
                results.add(resultSet);
            } while (s.getMoreResults(Statement.KEEP_CURRENT_RESULT));
            s.closeOnCompletion();
        } else {
            int[] counts = new int[]{s.getUpdateCount()};
            Pair pair = new Pair(counts, s.getGeneratedKeys());
            s.closeOnCompletion();
            results.add(pair);
        }
    }

    private PreparedStatement getCachedOrPrepare(String sql, @Nullable String[] keys) throws SQLException {
        PreparedStatement statement;
        if (keys == null) {
            statement = conn.prepareStatement(sql);
        } else {
            if (keys.length == 0) {
                statement = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            } else {
                statement = conn.prepareStatement(sql, keys);
            }
        }
        return statement;
    }

    private void bindStatement(PreparedStatement s, Map<Integer, Object> map) throws SQLException {
        s.clearParameters();
        for (int i = 0; i < map.size(); i++) {
            s.setObject(i + 1, codec.encode(conn, map.getOrDefault(i, null)));
        }
    }

    @Override
    public void run() {
        Thread current = Thread.currentThread();
        current.setName("R2jdbcWorker-" + current.getId());

        try {
            codec = initCodec();
        } catch (ClassNotFoundException | ClassCastException
                | InvocationTargetException | NoSuchMethodException
                | IllegalAccessException | InstantiationException e) {
            synchronized (this) {
                state = State.ENDED;
            }
            log.error("Failed to instantiate Codec", e);
            return;
        }

        log.debug("Listening");
        try {
            while (!Thread.interrupted()) {
                takeAndProcess();
            }
        } catch (InterruptedException ignored) {
        }
        log.debug("Cleaning up");
        while (jobs.peek() != null) {
            try {
                takeAndProcess();
            } catch (InterruptedException ignored) {
            }
        }
        if (conn != null) {
            try {
                conn.commit();
            } catch (SQLException e) {
                log.error("Error committing the last commit", e);
            }
            try {
                conn.close();
            } catch (SQLException e) {
                log.error("Error closing database", e);
            }
        }
        closeJobs.forEach(job -> offer(job.consumer));
        closeJobs.clear();
        metadata.set(null);
        log.debug("Exiting");
    }

    public Mono<Void> close() {
        if (refCount.decrementAndGet() == 0) {
            if (!shared || pollDuration.isZero()) {
                return closeNow();
            }
        }
        return Mono.empty();
    }

    public Mono<Void> closeNow() {
        return Mono.defer(() -> {
            synchronized (this) {
                if (state == State.STARTING) {
                    Mono<Void> voidMono = JdbcWorker.voidSend(this, JdbcJob.Job.CLOSE, null);
                    state = State.CLOSING;
                    return voidMono;
                } else {
                    return Mono.empty();
                }
            }
        });
    }

    public BlockingQueue<JdbcJob> getJobQueue() {
        return jobs;
    }

    public synchronized boolean isAlive() {
        return state == State.STARTING;
    }

    public Mono<JdbcConnectionMetadata> start() {
        refCount.incrementAndGet();
        synchronized (this) {
            if (state == State.RUNNABLE) {
                state = State.STARTING;
                thread.start();
                return send(this, JdbcJob.Job.INIT, null, packet -> (JdbcConnectionMetadata) packet.data)
                        .doOnNext(metadata::set);
            } else if (state == State.STARTING) {
                return metadata.get();
            } else {
                return Mono.error(new IllegalStateException("Thread ended"));
            }
        }
    }

    enum State {
        RUNNABLE, STARTING, CLOSING, ENDED,
    }
}
