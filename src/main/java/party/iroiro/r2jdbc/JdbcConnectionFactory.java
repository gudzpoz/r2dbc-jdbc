package party.iroiro.r2jdbc;

import io.r2dbc.spi.Closeable;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.ConnectionFactoryOptions;
import lbmq.LinkedBlockingMultiQueue;
import lombok.extern.slf4j.Slf4j;
import party.iroiro.lock.Lock;
import party.iroiro.lock.ReactiveLock;
import party.iroiro.r2jdbc.util.QueueDispatcher;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class JdbcConnectionFactory implements ConnectionFactory, Closeable {
    private final ConnectionFactoryOptions options;
    private final QueueDispatcher<JdbcPacket> adapter;
    private final Thread dispatcher;
    private final boolean shared;
    private final Lock workerLock;
    private final AtomicReference<JdbcWorker> sharedWorker;

    JdbcConnectionFactory(ConnectionFactoryOptions options) {
        this.options = options;
        this.adapter = new QueueDispatcher<>(new LinkedBlockingMultiQueue<>());
        this.dispatcher = new Thread(this.adapter);
        this.shared = options.hasOption(JdbcConnectionFactoryProvider.SHARED);
        sharedWorker = new AtomicReference<>();
        workerLock = new ReactiveLock();
    }

    private Mono<Void> initDispatcher() {
        return Mono.fromCallable(() -> {
            if (!dispatcher.isAlive()) {
                dispatcher.start();
            }
            return null;
        });
    }

    @Override
    public Mono<JdbcConnection> create() {
        return initDispatcher()
                .doOnTerminate(workerLock::lock)
                .then(Mono.fromSupplier(() -> {
                    JdbcWorker jdbcWorker = sharedWorker.get();
                    if (!shared || jdbcWorker == null || !jdbcWorker.isAlive()) {
                        JdbcConnection jdbcConnection = new JdbcConnection(adapter, options);
                        if (shared) {
                            sharedWorker.set(jdbcConnection.getWorker());
                        }
                        return jdbcConnection;
                    } else {
                        return new JdbcConnection(jdbcWorker, options);
                    }
                }))
                .transform(workerLock::unlockOnTerminate)
                .flatMap(JdbcConnection::init);
    }

    @Override
    public ConnectionFactoryMetadata getMetadata() {
        return JdbcConnectionFactoryMetadata.INSTANCE;
    }

    public Mono<Void> close() {
        return workerLock.lock()
                .then(Mono.fromSupplier(sharedWorker::get))
                .flatMap(worker -> {
                    if (worker != null && shared) {
                        log.debug("Closing factory");
                        return worker.closeNow();
                    } else {
                        return Mono.empty();
                    }
                }).transform(workerLock::unlockOnTerminate)
                .doOnTerminate(dispatcher::interrupt);
    }
}
