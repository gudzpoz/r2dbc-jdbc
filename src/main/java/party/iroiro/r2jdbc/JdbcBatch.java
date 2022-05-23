package party.iroiro.r2jdbc;

import io.r2dbc.spi.Batch;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;

public class JdbcBatch implements Batch {
    private final JdbcConnection conn;

    final ArrayList<String> sql;

    JdbcBatch(JdbcConnection conn) {
        if (conn == null) {
            throw new IllegalArgumentException("connection must not be null");
        }
        this.conn = conn;
        sql = new ArrayList<>();
    }

    @Override
    public JdbcBatch add(String sql) {
        //noinspection ConstantConditions
        if (sql == null) {
            throw new IllegalArgumentException("sql must not be null");
        }
        this.sql.add(sql);
        return this;
    }

    @Override
    public Flux<JdbcResult> execute() {
        return conn.send(JdbcJob.Job.BATCH, this, packet -> {
            assert packet.data instanceof List;
            return (List<?>) packet.data;
        }).flatMapMany(list -> Flux.fromStream(list.stream().map(
                data -> new JdbcResult(conn, data, conn.getConverter()))));
    }
}
