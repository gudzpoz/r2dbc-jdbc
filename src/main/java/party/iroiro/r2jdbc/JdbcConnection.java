package party.iroiro.r2jdbc;

import io.r2dbc.spi.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.ConstructorUtils;
import org.reactivestreams.Publisher;
import party.iroiro.r2jdbc.codecs.Converter;
import party.iroiro.r2jdbc.codecs.DefaultConverter;
import party.iroiro.r2jdbc.util.QueueDispatcher;
import reactor.core.publisher.Mono;

import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

@Slf4j
public class JdbcConnection implements Connection {
    private final AtomicReference<ConnectionMetadata> metadata;
    private final AtomicBoolean autoCommit;
    private final AtomicBoolean valid;
    private final AtomicReference<IsolationLevel> isolationLevel;
    private final JdbcWorker worker;
    private final Converter converter;

    JdbcConnection(JdbcWorker worker, ConnectionFactoryOptions options)
            throws JdbcException {
        this.worker = worker;

        autoCommit = new AtomicBoolean(true);
        valid = new AtomicBoolean(true);
        isolationLevel = new AtomicReference<>();
        metadata = new AtomicReference<>();
        try {
            converter = getConverter(options);
        } catch (ClassNotFoundException | InstantiationException | ClassCastException
                | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            throw new JdbcException(e);
        }
    }

    JdbcConnection(QueueDispatcher<JdbcPacket> adapter, ConnectionFactoryOptions options) {
        this(new JdbcWorker(new LinkedBlockingDeque<>(), adapter.subQueue(), options), options);
    }

    private static Converter getConverter(ConnectionFactoryOptions options)
            throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, IllegalAccessException, InstantiationException {
        if (options.hasOption(JdbcConnectionFactoryProvider.CONV)) {
            Class<?> aClass = Class.forName((String) options.getValue(JdbcConnectionFactoryProvider.CONV));

            if (Converter.class.isAssignableFrom(aClass)) {
                return (Converter) ConstructorUtils.invokeConstructor(aClass, null);
            } else {
                throw new ClassCastException(aClass.getName());
            }
        } else {
            return new DefaultConverter();
        }
    }

    Mono<JdbcConnection> init() {
        return worker.start().doOnNext(metadata::set).thenReturn(this);
    }

    Mono<Void> voidSend(JdbcJob.Job job, @Nullable Object data) {
        boolean v = valid.get();
        if (!v) {
            return Mono.empty();
        }
        return JdbcWorker.voidSend(worker, job, data);
    }

    boolean offerNow(JdbcJob.Job job, @Nullable Object data, BiConsumer<JdbcPacket, Exception> consumer) {
        return JdbcWorker.offerNow(worker, job, data, consumer);
    }

    <T> Mono<T> send(JdbcJob.Job job, @Nullable Object data, Function<JdbcPacket, T> converter) {
        if (!valid.get()) {
            return Mono.empty();
        }
        return JdbcWorker.send(worker, job, data, converter);
    }

    @Override
    public Mono<Void> beginTransaction() {
        autoCommit.set(false);
        return voidSend(JdbcJob.Job.START_TRANSACTION, null);
    }

    @Override
    public Mono<Void> beginTransaction(TransactionDefinition definition) {
        return beginTransaction();
    }

    @Override
    public Mono<Void> close() {
        return Mono.fromRunnable(() ->
                valid.set(false)).then(Mono.defer(worker::close));
    }

    @Override
    public Mono<Void> commitTransaction() {
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
    public JdbcStatement createStatement(String sql) {
        //noinspection ConstantConditions
        if (sql == null) {
            throw new IllegalArgumentException("Null statement not allowed");
        }
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
    public Mono<Void> rollbackTransaction() {
        return voidSend(JdbcJob.Job.ROLLBACK_TRANSACTION, null);
    }

    @Override
    public Publisher<Void> rollbackTransactionToSavepoint(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Mono<Void> setAutoCommit(boolean autoCommit) {
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
    public Mono<Void> setLockWaitTimeout(Duration timeout) {
        return Mono.empty();
    }

    /**
     * No-op
     *
     * @param timeout ignored
     * @return {@link Mono#empty()}
     */
    @Override
    public Mono<Void> setStatementTimeout(Duration timeout) {
        return Mono.empty();
    }

    @Override
    public Mono<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
        this.isolationLevel.set(isolationLevel);
        return voidSend(JdbcJob.Job.SET_ISOLATION_LEVEL, isolationLevel);
    }

    @Override
    public Mono<Boolean> validate(ValidationDepth depth) {
        return Mono.fromSupplier(valid::get)
                .flatMap(v -> {
                    if (v && depth == ValidationDepth.REMOTE) {
                        return send(JdbcJob.Job.VALIDATE, null, packet -> (Boolean) packet.data);
                    } else {
                        return Mono.just(v);
                    }
                });
    }

    public JdbcWorker getWorker() {
        return worker;
    }

    public Converter getConverter() {
        return converter;
    }
}
