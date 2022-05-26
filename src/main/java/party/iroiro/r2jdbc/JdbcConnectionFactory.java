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

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class JdbcConnectionFactory implements ConnectionFactory, Closeable {
    private final ConnectionFactoryOptions options;
    private final QueueDispatcher<JdbcPacket> adapter;
    private final Thread dispatcher;
    private final Lock workerLock;
    private final AtomicReference<JdbcWorker> sharedWorker;

    JdbcConnectionFactory(ConnectionFactoryOptions options) {
        this.options = options;
        this.adapter = new QueueDispatcher<>(new LinkedBlockingMultiQueue<>());
        this.dispatcher = new Thread(this.adapter);
        sharedWorker = new AtomicReference<>();
        workerLock = new ReactiveLock();
    }

    private Mono<Void> init() {
        return workerLock.lock().doOnSuccess((v) -> {
            if (sharedWorker.get() == null) {
                dispatcher.start();
                sharedWorker.set(new JdbcWorker(
                        new LinkedBlockingDeque<>(), adapter.subQueue(), options
                ));
            }
        }).transform(workerLock::unlockOnTerminate);
    }

    @Override
    public Mono<JdbcConnection> create() {
        return init()
                .then(Mono.fromSupplier(() -> {
                    JdbcWorker jdbcWorker = sharedWorker.get();
                    return new JdbcConnection(jdbcWorker, options);
                }))
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
                    if (worker != null) {
                        log.debug("Closing factory");
                        return worker.closeNow().doOnTerminate(dispatcher::interrupt);
                    } else {
                        return Mono.empty();
                    }
                }).transform(workerLock::unlockOnTerminate)
                .doOnTerminate(dispatcher::interrupt);
    }
}
