package party.iroiro.r2jdbc;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.IsolationLevel;
import lbmq.LinkedBlockingMultiQueue;
import lombok.extern.slf4j.Slf4j;
import party.iroiro.r2jdbc.util.Pair;
import party.iroiro.r2jdbc.util.QueueItem;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.sql.*;
import java.time.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

@Slf4j
class JdbcWorker implements Runnable {
    private static final ConcurrentHashMap<Integer, Class<?>> columnTypeGuesses;

    static {
        columnTypeGuesses = new ConcurrentHashMap<>();
        put(Void.class, Types.NULL); // FIXME: Probably not
        put(String.class, Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR,
                Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR);
        put(BigDecimal.class, Types.NUMERIC, Types.DECIMAL);
        put(Long.class, Types.BIGINT);
        put(Integer.class, Types.INTEGER, Types.TINYINT, Types.SMALLINT);
        put(Float.class, Types.FLOAT, Types.REAL);
        put(Double.class, Types.DOUBLE);
        put(byte[].class, Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY);
        put(Boolean.class, Types.BOOLEAN, Types.BIT);
        put(LocalDate.class, Types.DATE);
        put(LocalTime.class, Types.TIME);
        put(OffsetTime.class, Types.TIME_WITH_TIMEZONE);
        put(LocalDateTime.class, Types.TIMESTAMP);
        put(Instant.class, Types.TIMESTAMP_WITH_TIMEZONE);
        put(Object.class, Types.JAVA_OBJECT, Types.OTHER);
        put(Object[].class, Types.ARRAY);
        // TODO: Clob, Blob?
    }

    private final BlockingQueue<JdbcJob> jobs;
    private final LinkedBlockingMultiQueue<Integer, QueueItem<JdbcPacket>>.SubQueue out;
    private final ConnectionFactoryOptions options;
    private final Thread thread;
    private final AtomicBoolean starting;
    private final AtomicReference<JdbcConnectionMetadata> metadata;
    private final AtomicInteger refCount;
    private final Duration pollDuration;
    private final boolean shared;
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
        metadata = new AtomicReference<>();
        starting = new AtomicBoolean(false);
        refCount = new AtomicInteger(0);
        shared = options.hasOption(JdbcConnectionFactoryProvider.SHARED);
    }

    private static void put(Class<?> clazz, int... types) {
        for (int type : types) {
            columnTypeGuesses.put(type, clazz);
        }
    }

    private static Connection getConnection(ConnectionFactoryOptions options) throws SQLException {
        JdbcConnectionFactoryProvider.JdbcConnectionDetails details =
                JdbcConnectionFactoryProvider.getJdbcConnectionUrl(options);
        return DriverManager.getConnection(details.getUrl(), details.getProperties());
    }

    static Mono<Void> voidSend(JdbcWorker worker, JdbcJob.Job job, Object data) {
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
                            Object data,
                            BiConsumer<JdbcPacket, Exception> consumer) {
        if (worker.isAlive()) {
            // FIXME: Edge cases
            return worker.getJobQueue().offer(new JdbcJob(job, data, consumer));
        } else {
            return false;
        }
    }

    static <T> Mono<T> send(JdbcWorker worker, JdbcJob.Job job, Object data, Function<JdbcPacket, T> converter) {
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

    private void offer(JdbcPacket packet, BiConsumer<JdbcPacket, Exception> consumer) {
        out.offer(new QueueItem<>(packet, null, consumer, true));
    }

    private void offer(BiConsumer<JdbcPacket, Exception> consumer) {
        out.offer(new QueueItem<>(null, null, consumer, false));
    }

    private void offer(Exception e, BiConsumer<JdbcPacket, Exception> consumer) {
        out.offer(new QueueItem<>(
                null, e, consumer, true
        ));
    }

    private void takeAndProcess() throws InterruptedException {
        JdbcJob job = pollDuration.isZero() ?
                jobs.take() : jobs.poll(pollDuration.toMillis(), TimeUnit.MILLISECONDS);
        if (job == null) {
            if (refCount.get() == 0) {
                if (shared) {
                    starting.set(false);
                    throw new InterruptedException("Closing after no connection for sometime");
                }
            }
            return;
        }
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
                try {
                    ResultSetMetaData metadata = result.getMetaData();
                    int count = metadata.getColumnCount();
                    ArrayList<JdbcColumnMetadata> columns = new ArrayList<>(count);
                    for (int i = 0; i < count; i++) {
                        log.trace("Column Type: {}", metadata.getColumnType(i + 1));
                        columns.add(new JdbcColumnMetadata(
                                new JdbcColumnMetadata.JdbcColumnType(
                                        columnTypeGuesses
                                                .getOrDefault(metadata.getColumnType(i + 1),
                                                        null),
                                        metadata.getColumnTypeName(i + 1)
                                ),
                                metadata.getColumnName(i + 1)
                        ));
                    }
                    offer(new JdbcPacket(new JdbcRowMetadata(columns)), job.consumer);
                } catch (SQLException e) {
                    offer(e, job.consumer);
                }
                break;
            case RESULT_ROWS:
                JdbcResult.JdbcResultRequest request = (JdbcResult.JdbcResultRequest) job.data;
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
                            Class<?> type = metadata.get(j).getJavaType();
                            if (type == null) {
                                rowData.add(row.getObject(j + 1));
                            } else {
                                rowData.add(row.getObject(j + 1, type));
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
                starting.set(false);
                offer(job.consumer);
                throw new InterruptedException("Connection closing");
        }
        log.trace("Process finished: {}", job.job);
    }

    private Object execute(String sql, ArrayList<Map<Integer, Object>> bindings, String[] keys) throws SQLException {
        PreparedStatement s = getCachedOrPrepare(sql, keys);
        if (bindings == null || bindings.size() == 1) {
            if (bindings != null) {
                bindStatement(s, bindings.get(0));
            }
            boolean isQuery = s.execute();
            if (isQuery) {
                ResultSet resultSet = s.getResultSet();
                s.closeOnCompletion();
                return resultSet;
            } else {
                int[] counts = new int[]{s.getUpdateCount()};
                Pair pair = new Pair(counts, s.getGeneratedKeys());
                s.closeOnCompletion();
                return pair;
            }
        } else if (bindings.size() == 0) {
            s.close();
            throw new IllegalArgumentException("No valid statement");
        } else {
            for (var map : bindings) {
                bindStatement(s, map);
                s.addBatch();
            }
            int[] ints = s.executeBatch();
            s.close();
            return ints;
        }
    }

    private PreparedStatement getCachedOrPrepare(String sql, String[] keys) throws SQLException {
        PreparedStatement statement;
        if (keys == null) {
            statement = conn.prepareStatement(sql);
        } else {
            statement = conn.prepareStatement(sql, keys);
        }
        return statement;
    }

    private void bindStatement(PreparedStatement s, Map<Integer, Object> map) throws SQLException {
        for (int i = 0; i < map.size(); i++) {
            s.setObject(i + 1, map.getOrDefault(i, null));
        }
    }

    @Override
    public void run() {
        Thread current = Thread.currentThread();
        current.setName("R2jdbcWorker-" + current.getId());
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
        return Mono.fromCallable(starting::get).flatMap(starting -> {
            if (starting) {
                return JdbcWorker.voidSend(this, JdbcJob.Job.CLOSE, null);
            } else {
                return Mono.empty();
            }
        });
    }

    public BlockingQueue<JdbcJob> getJobQueue() {
        return jobs;
    }

    public boolean isAlive() {
        return starting.get() && thread.isAlive();
    }

    public Mono<JdbcConnectionMetadata> start() {
        refCount.incrementAndGet();
        if (!starting.getAndSet(true) && !thread.isAlive()) {
            try {
                thread.start();
                return send(this, JdbcJob.Job.INIT, null, packet -> (JdbcConnectionMetadata) packet.data)
                        .doOnNext(metadata::set);
            } catch (IllegalThreadStateException ignored) {
            }
        }
        return Mono.just(metadata.get());
    }
}
