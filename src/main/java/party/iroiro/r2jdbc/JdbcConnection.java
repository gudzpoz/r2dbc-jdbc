package party.iroiro.r2jdbc;

import io.r2dbc.spi.*;
import org.reactivestreams.Publisher;
import party.iroiro.r2jdbc.util.QueueDispatcher;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class JdbcConnection implements Connection {
    private final BlockingQueue<JdbcJob> jobs;
    private final AtomicReference<ConnectionMetadata> metadata;
    private final AtomicBoolean autoCommit;
    private final AtomicBoolean valid;
    private final AtomicReference<IsolationLevel> isolationLevel;
    private final QueueDispatcher<JdbcPacket> adapter;
    private final ConnectionFactoryOptions options;

    JdbcConnection(QueueDispatcher<JdbcPacket> adapter, ConnectionFactoryOptions options) {
        this.adapter = adapter;
        this.options = options;
        this.jobs = new LinkedBlockingDeque<>();

        autoCommit = new AtomicBoolean(true);
        valid = new AtomicBoolean(true);
        isolationLevel = new AtomicReference<>();
        metadata = new AtomicReference<>();
    }

    Mono<JdbcConnection> init() {
        new Thread(new JdbcWorker(jobs, adapter.subQueue(), options)).start();
        return send(JdbcJob.Job.INIT, null, packet -> (JdbcConnectionMetadata) packet.data)
                .doOnNext(metadata::set).thenReturn(this);
    }

    Mono<Void> voidSend(JdbcJob.Job job, Object data) {
        boolean v = valid.get();
        if (!v) {
            return Mono.empty();
        }
        return voidSendValid(job, data);
    }

    Mono<Void> voidSendValid(JdbcJob.Job job, Object data) {
        return Mono.create(voidMonoSink -> voidMonoSink.onRequest(
                ignored -> {
                    if (!offerNow(job, data, (i, e) -> {
                        if (e == null) {
                            voidMonoSink.success();
                        } else {
                            voidMonoSink.error(new JdbcException(e));
                        }
                    })) {
                        voidMonoSink.error(new IndexOutOfBoundsException("Unable to push to queue"));
                    }
                }));
    }

    boolean offerNow(JdbcJob.Job job, Object data, BiConsumer<JdbcPacket, Exception> consumer) {
        return jobs.offer(new JdbcJob(job, data, consumer));
    }

    <T> Mono<T> send(JdbcJob.Job job, Object data, Function<JdbcPacket, T> converter) {
        if (!valid.get()) {
            return Mono.empty();
        }
        return Mono.create(sink -> sink.onRequest(
                ignored -> {
                    if (!offerNow(job, data, (i, e) -> {
                        if (e == null) {
                            sink.success(converter.apply(i));
                        } else {
                            sink.error(new JdbcException(e));
                        }
                    })) {
                        sink.error(new IndexOutOfBoundsException("Unable to push to queue"));
                    }
                }));
    }

    @Override
    public Publisher<Void> beginTransaction() {
        autoCommit.set(false);
        return voidSend(JdbcJob.Job.START_TRANSACTION, null);
    }

    @Override
    public Publisher<Void> beginTransaction(TransactionDefinition definition) {
        return beginTransaction();
    }

    @Override
    public Mono<Void> close() {
        if (valid.compareAndSet(true, false)) {
            valid.set(false);
            return voidSendValid(JdbcJob.Job.CLOSE, null);
        } else {
            return Mono.empty();
        }
    }

    @Override
    public Publisher<Void> commitTransaction() {
        return voidSend(JdbcJob.Job.END_TRANSACTION, null);
    }

    @Override
    public Batch createBatch() {
        return new JdbcBatch(this);
    }

    @Override
    public Publisher<Void> createSavepoint(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Statement createStatement(String sql) {
        return new JdbcStatement(sql, this);
    }

    @Override
    public boolean isAutoCommit() {
        return autoCommit.get();
    }

    @Override
    public ConnectionMetadata getMetadata() {
        return metadata.get();
    }

    @Override
    public IsolationLevel getTransactionIsolationLevel() {
        return isolationLevel.get();
    }

    @Override
    public Publisher<Void> releaseSavepoint(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Publisher<Void> rollbackTransaction() {
        return voidSend(JdbcJob.Job.ROLLBACK_TRANSACTION, null);
    }

    @Override
    public Publisher<Void> rollbackTransactionToSavepoint(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Publisher<Void> setAutoCommit(boolean autoCommit) {
        return voidSend(JdbcJob.Job.SET_AUTO_COMMIT, autoCommit)
                .then(send(JdbcJob.Job.GET_AUTO_COMMIT, null, (packet -> (Boolean) packet.data)))
                .doOnNext(this.autoCommit::set).then();
    }

    /**
     * No-op
     *
     * @param timeout ignored
     * @return {@link Mono#empty()}
     */
    @Override
    public Publisher<Void> setLockWaitTimeout(Duration timeout) {
        return Mono.empty();
    }

    /**
     * No-op
     *
     * @param timeout ignored
     * @return {@link Mono#empty()}
     */
    @Override
    public Publisher<Void> setStatementTimeout(Duration timeout) {
        return Mono.empty();
    }

    @Override
    public Publisher<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
        this.isolationLevel.set(isolationLevel);
        return voidSend(JdbcJob.Job.SET_ISOLATION_LEVEL, isolationLevel);
    }

    @Override
    public Publisher<Boolean> validate(ValidationDepth depth) {
        boolean v = valid.get();
        if (depth == ValidationDepth.LOCAL || !v) {
            return Mono.just(v);
        } else {
            return send(JdbcJob.Job.VALIDATE, null, packet -> (Boolean) packet.data);
        }
    }
}
