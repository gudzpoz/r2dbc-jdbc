package party.iroiro.r2jdbc;

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Result;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;

public class JdbcBatch implements Batch {
    private final JdbcConnection conn;

    final ArrayList<String> sql;

    public JdbcBatch(JdbcConnection conn) {
        this.conn = conn;
        sql = new ArrayList<>();
    }

    @Override
    public Batch add(String sql) {
        this.sql.add(sql);
        return this;
    }

    @Override
    public Publisher<? extends Result> execute() {
        return conn.send(JdbcJob.Job.BATCH, this, packet -> {
            assert packet.data instanceof List;
            return (List<?>) packet.data;
        }).flatMapMany(list -> Flux.fromStream(list.stream().map(data -> new JdbcResult(conn, data))));
    }
}
