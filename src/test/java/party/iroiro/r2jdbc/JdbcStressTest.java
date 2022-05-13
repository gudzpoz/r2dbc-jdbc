package party.iroiro.r2jdbc;

import io.r2dbc.spi.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Instant;
import java.util.ArrayList;

@Slf4j
public class JdbcStressTest {
    @Test
    public void notBatchedStressTest() {
        final int count = 10000;
        final int threads = 8;
        ConnectionFactory factory = ConnectionFactories.get("r2dbc:r2jdbc:h2~:////tmp/test");
        long start = System.nanoTime();
        Mono.from(factory.create())
                .flatMap(this::init)
                .flatMapMany(connection -> {
                    ArrayList<Flux<Integer>> fluxes = new ArrayList<>(threads);
                    Scheduler stress = Schedulers
                            .newBoundedElastic(10, 10, "Stress");
                    long now = Instant.now().toEpochMilli();
                    for (int i = 0; i < threads; i++) {
                        final int ii = i;
                        fluxes.add(Mono.just(connection).publishOn(stress)
                                .flatMapMany(conn -> {
                                    ArrayList<Mono<Integer>> results = new ArrayList<>(count);
                                    for (int j = 0; j < count; j++) {
                                        int index = ii * count + j;
                                        results.add(insertIt(conn, index + now, index));
                                    }
                                    return Flux.merge(results);
                                }));
                    }
                    return Flux.merge(fluxes);
                }).blockLast();
        long end = System.nanoTime();
        log.info("Time (ns): {}", end - start);
    }

    @Test
    public void batchedStressTest() {
        final int count = 10000;
        final int threads = 8;
        ConnectionFactory factory = ConnectionFactories.get("r2dbc:r2jdbc:h2~:////tmp/test");
        long start = System.nanoTime();
        Mono.from(factory.create())
                .flatMap(this::init)
                .flatMapMany(connection -> {
                    ArrayList<Mono<Integer>> fluxes = new ArrayList<>(threads);
                    Scheduler stress = Schedulers
                            .newBoundedElastic(10, 10, "Stress");
                    long now = Instant.now().toEpochMilli();
                    for (int i = 0; i < threads; i++) {
                        Statement statement =
                                connection.createStatement("insert into s_test (id, name) " + "values (?, ?)");
                        for (int j = 0; j < count; j++) {
                            int index = i * count + j;
                            statement.bind(0, index + now).bind(1, "Batch-" + index);
                            if (j != count - 1) {
                                statement.add();
                            }
                        }
                        fluxes.add(Flux.from(statement.execute()).publishOn(stress)
                                .flatMap(Result::getRowsUpdated)
                                .map(l -> (int) (long) l).last().doOnNext(ignored -> log.debug("Finished")));
                    }
                    return Flux.merge(fluxes);
                }).blockLast();
        long end = System.nanoTime();
        log.info("Time (ns): {}", end - start);
    }

    private Mono<Integer> insertIt(Connection conn, long timed, int index) {
        return Mono.from(conn.createStatement("insert into s_test (id, name) " +
                "values (?, ?)")
                .bind(0, timed)
                .bind(1, "Stress-" + index)
                .execute()).flatMap(result ->
                Mono.from(result.getRowsUpdated()).map(l -> (int) (long) l));
    }

    private Mono<Connection> init(Connection connection) {
        return Mono.from(connection.createStatement("drop table if exists s_test").execute())
                .thenMany(connection.createStatement("create table s_test" +
                        "(id bigint primary key, name varchar)").execute())
                .last().thenReturn(connection);
    }
}
