package party.iroiro.r2jdbc;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.IsolationLevel;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.ConstructorUtils;
import party.iroiro.r2jdbc.codecs.Codec;
import party.iroiro.r2jdbc.codecs.DefaultCodec;
import party.iroiro.r2jdbc.util.Pair;
import party.iroiro.r2jdbc.util.QueueItem;
import party.iroiro.r2jdbc.util.SemiBlockingQueue;
import party.iroiro.r2jdbc.util.SingletonMono;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Function;

@Slf4j
class JdbcWorker implements Runnable {
    private final SemiBlockingQueue<JdbcJob> jobs;
    private final SemiBlockingQueue<QueueItem<JdbcPacket>> out;
    private final ConnectionFactoryOptions options;
    private final SingletonMono<JdbcConnectionMetadata> metadata;
    private final List<JdbcJob> closeJobs;
    private State state;
    private Codec codec;
    private final List<Connection> connections;
    private final ReentrantLock shutdownLock;

    JdbcWorker(SemiBlockingQueue<JdbcJob> jobs,
               SemiBlockingQueue<QueueItem<JdbcPacket>> out,
               ConnectionFactoryOptions options) {
        this.jobs = jobs;
        this.out = out;
        this.options = options;
        this.connections = new LinkedList<>();
        Thread thread = new Thread(this);
        thread.setDaemon(true);
        metadata = new SingletonMono<>();
        codec = null;
        closeJobs = new LinkedList<>();
        state = State.STARTING;

        thread.start();
        shutdownLock = new ReentrantLock();
    }

    private static Connection getConnection(ConnectionFactoryOptions options) throws SQLException {
        JdbcConnectionFactoryProvider.JdbcConnectionDetails details =
                JdbcConnectionFactoryProvider.getJdbcConnectionUrl(options);
        return DriverManager.getConnection(details.getUrl(), details.getProperties());
    }

    static Mono<Void> voidSend(JdbcWorker worker, @Nullable Connection connection,
                               JdbcJob.Job job, @Nullable Object data) {
        return Mono.create(voidMonoSink -> voidMonoSink.onRequest(
                ignored -> {
                    if (!offerNow(worker, connection, job, data, (i, e) -> {
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
                            @Nullable Connection connection,
                            JdbcJob.Job job,
                            @Nullable Object data,
                            BiConsumer<JdbcPacket, Throwable> consumer) {
        if (worker.notEnded()) {
            // FIXME: Edge cases
            worker.getJobQueue().offer(new JdbcJob(connection, job, data, consumer));
            return true;
        } else {
            return false;
        }
    }

    static <T> Mono<T> send(JdbcWorker worker, @Nullable Connection connection, JdbcJob.Job job,
                            @Nullable Object data, Function<JdbcPacket, T> converter) {
        return Mono.create(sink -> sink.onRequest(
                ignored -> {
                    if (!offerNow(worker, connection, job, data, (i, e) -> {
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

    public synchronized boolean notEnded() {
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
        out.offer(new QueueItem<>(packet, null, consumer));
    }

    private void offer(BiConsumer<JdbcPacket, Throwable> consumer) {
        out.offer(new QueueItem<>(null, null, consumer));
    }

    private void offer(Exception e, BiConsumer<JdbcPacket, Throwable> consumer) {
        out.offer(new QueueItem<>(null, e, consumer));
    }

    private void takeAndProcess() throws InterruptedException {
        JdbcJob job = jobs.take();
        try {
            process(job);
        } catch (Throwable any) {
            log.error("Unexpected exception", any);
            offer(new JdbcException(any), job.consumer);
        }
    }

    private void process(JdbcJob job) {
        log.trace("Processing: {}", job.job);
        Connection conn = job.connection;
        switch (job.job) {
            case INIT_CONNECTION:
                try {
                    conn = getConnection(options);
                    connections.add(conn);
                    DatabaseMetaData metaData = conn.getMetaData();
                    offer(new JdbcPacket(
                            new JdbcConnectionMetadata(
                                    conn,
                                    metaData.getDatabaseProductName(),
                                    metaData.getDatabaseProductVersion()
                            )
                    ), job.consumer);
                } catch (SQLException e) {
                    offer(e, job.consumer);
                }
                break;
            case CLOSE_CONNECTION:
                try {
                    commitAndClose(conn);
                    connections.remove(conn);
                    offer(job.consumer);
                } catch (SQLException e) {
                    offer(e, job.consumer);
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
                if (statement.getSize() == 0) {
                    offer(new IllegalArgumentException("Fetch size is either -1 or positive"), job.consumer);
                } else {
                    try {
                        Object result = execute(conn, statement.sql,
                                statement.bindings, statement.wantsGenerated.get());
                        offer(new JdbcPacket(result), job.consumer);
                    } catch (SQLException | IllegalArgumentException e) {
                        offer(e, job.consumer);
                    }
                }
                break;
            case BATCH:
                JdbcBatch batch = (JdbcBatch) job.data;
                List<Object> results = new ArrayList<>(batch.sql.size());
                for (String s : batch.sql) {
                    try {
                        Object result = execute(conn, s, null, null);
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
                            if (type == null || type.isArray()) {
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
        }
        log.trace("Process finished: {}", job.job);
    }

    private Object execute(Connection conn, String sql, @Nullable ArrayList<Map<Integer, Object>> bindings,
                           @Nullable String[] keys) throws SQLException {
        PreparedStatement s = getCachedOrPrepare(conn, sql, keys);
        ArrayList<Object> results = new ArrayList<>(bindings == null ? 1 : bindings.size());
        if (bindings == null) {
            executeSingle(conn, results, s, null);
        } else if (bindings.size() == 0) {
            s.close();
            throw new IllegalArgumentException("No valid statement");
        } else {
            for (Map<Integer, Object> binding : bindings) {
                executeSingle(conn, results, s, binding);
            }
        }
        return results;
    }

    private void executeSingle(Connection conn, ArrayList<Object> results, PreparedStatement s,
                               @Nullable Map<Integer, Object> binding) throws SQLException {
        if (binding != null) {
            bindStatement(conn, s, binding);
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

    private PreparedStatement getCachedOrPrepare(Connection conn,
                                                 String sql,
                                                 @Nullable String[] keys) throws SQLException {
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

    private void bindStatement(Connection conn,
                               PreparedStatement s,
                               Map<Integer, Object> map) throws SQLException {
        s.clearParameters();
        for (int i = 0; i < map.size(); i++) {
            s.setObject(i + 1, codec.encode(conn, map.getOrDefault(i, null)));
        }
    }

    @Override
    public void run() {
        Thread current = Thread.currentThread();
        jobs.setConsumer(current);
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

        Thread shutdownHook = registerShutdown();

        try {
            shutdownLock.lock();

            log.debug("Listening");
            try {
                while (state == State.STARTING) {
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
            closeAllConnections();
            closeJobs.forEach(job -> offer(job.consumer));
            closeJobs.clear();
            metadata.set(null);
            log.debug("Exiting");

            deregisterShutdown(shutdownHook);

        } finally {
            shutdownLock.unlock();
        }
    }

    private void deregisterShutdown(@Nullable Thread shutdownHook) {
        if (shutdownHook != null) {
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (IllegalStateException ignored) {
            }
        }
    }

    @Nullable
    private Thread registerShutdown() {
        Thread shutdownHook = new Thread(this::shutdown);
        try {
            Runtime.getRuntime().addShutdownHook(shutdownHook);
            return shutdownHook;
        } catch (IllegalStateException e) {
            return null;
        }
    }

    private void shutdown() {
        if (state == State.STARTING) {
            try {
                offerNow(this, null, JdbcJob.Job.CLOSE, null, (a, b) -> {});
                shutdownLock.lock();
            } catch (Throwable ignored) {
            }
        }
    }

    private void commitAndClose(Connection connection) throws SQLException {
        connection.close();
    }

    private void closeAllConnections() {
        for (Connection conn : connections) {
            try {
                commitAndClose(conn);
            } catch (SQLException e) {
                log.error("Error closing database", e);
            }
        }
        connections.clear();
    }

    public Mono<Void> close(Connection connection) {
        return JdbcWorker.voidSend(this, connection, JdbcJob.Job.CLOSE_CONNECTION, null);
    }

    public Mono<Void> closeNow() {
        return Mono.defer(() -> JdbcWorker.voidSend(this,
                null, JdbcJob.Job.CLOSE, null));
    }

    public SemiBlockingQueue<JdbcJob> getJobQueue() {
        return jobs;
    }

    public synchronized boolean isAlive() {
        return state == State.STARTING;
    }

    public Mono<JdbcConnectionMetadata> newConnection() {
        if (state == State.STARTING) {
            return send(this, null,
                    JdbcJob.Job.INIT_CONNECTION, null, packet -> (JdbcConnectionMetadata) packet.data)
                    .doOnNext(metadata::set);
        } else {
            return Mono.error(new IllegalStateException("Thread ended"));
        }
    }

    enum State {
        STARTING, ENDED,
    }
}
