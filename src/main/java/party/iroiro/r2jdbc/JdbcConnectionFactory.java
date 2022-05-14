package party.iroiro.r2jdbc;

import io.r2dbc.spi.Closeable;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.ConnectionFactoryOptions;
import lbmq.LinkedBlockingMultiQueue;
import lombok.extern.slf4j.Slf4j;
import party.iroiro.r2jdbc.util.QueueDispatcher;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class JdbcConnectionFactory implements ConnectionFactory, Closeable {
    private final ConnectionFactoryOptions options;
    private final QueueDispatcher<JdbcPacket> adapter;
    private final Thread dispatcher;
    private final boolean shared;
    private final AtomicReference<JdbcWorker> sharedWorker;

    JdbcConnectionFactory(ConnectionFactoryOptions options) {
        this.options = options;
        this.adapter = new QueueDispatcher<>(new LinkedBlockingMultiQueue<>());
        this.dispatcher = new Thread(this.adapter);
        this.shared = options.hasOption(JdbcConnectionFactoryProvider.SHARED);
        sharedWorker = new AtomicReference<>();
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
        try {
            AtomicReference<JdbcConnection> connection = new AtomicReference<>(null);
            sharedWorker.getAndUpdate(jdbcWorker -> {
                if (jdbcWorker == null) {
                    connection.set(new JdbcConnection(adapter, options));
                    return connection.get().getWorker();
                } else {
                    connection.set(new JdbcConnection(jdbcWorker));
                    return jdbcWorker;
                }
            });
            return initDispatcher().then(connection.get().init());
        } catch (Exception e) {
            return Mono.error(e);
        }
    }

    @Override
    public ConnectionFactoryMetadata getMetadata() {
        return JdbcConnectionFactoryMetadata.INSTANCE;
    }

    public Mono<Void> close() {
        JdbcWorker worker = sharedWorker.getAndSet(null);
        if (worker != null && shared) {
            log.debug("Closing factory");
            return worker.closeNow();
        } else {
            return Mono.empty();
        }
    }
}
