package party.iroiro.r2jdbc;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.ConnectionFactoryOptions;
import lbmq.LinkedBlockingMultiQueue;
import org.reactivestreams.Publisher;
import party.iroiro.r2jdbc.util.QueueDispatcher;
import reactor.core.publisher.Mono;

public class JdbcConnectionFactory implements ConnectionFactory {
    private final ConnectionFactoryOptions options;
    private final QueueDispatcher<JdbcPacket> adapter;
    private final Thread dispatcher;

    public JdbcConnectionFactory(ConnectionFactoryOptions options) {
        this.options = options;
        this.adapter = new QueueDispatcher<>(new LinkedBlockingMultiQueue<>());
        this.dispatcher = new Thread(this.adapter);
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
    public Publisher<? extends Connection> create() {
        try {
            JdbcConnection connection = new JdbcConnection(adapter, options);
            return initDispatcher().then(connection.init());
        } catch (Exception e) {
            return Mono.error(e);
        }
    }

    @Override
    public ConnectionFactoryMetadata getMetadata() {
        return JdbcConnectionFactoryMetadata.INSTANCE;
    }
}
