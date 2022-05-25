package party.iroiro.r2jdbc;

import io.r2dbc.spi.Batch;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;

public class JdbcBatch implements Batch {
    final ArrayList<String> sql;
    private final JdbcConnection conn;

    JdbcBatch(JdbcConnection conn) {
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
        return conn.send(JdbcJob.Job.BATCH, this, packet -> (List<?>) packet.data)
                .flatMapMany(list -> Flux.fromStream(list.stream().map(
                        data -> new JdbcResult(conn, data, conn.getConverter()))));
    }
}
