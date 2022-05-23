package party.iroiro.r2jdbc;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
public class JdbcStressTest {
    @Test
    public void notBatchedStressTest() {
        final int count = 10000;
        final int threads = 8;
        JdbcConnectionFactory factory = (JdbcConnectionFactory)
                ConnectionFactories.get("r2dbc:r2jdbc:h2~:////tmp/test");
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
        final int threads = 16;
        JdbcConnectionFactory factory = (JdbcConnectionFactory)
                ConnectionFactories.get("r2dbc:r2jdbc:h2~:////tmp/test");
        long start = System.nanoTime();
        long now = Instant.now().toEpochMilli();
        Connection conn = Mono.from(factory.create())
                .flatMap(this::init)
                .flatMapMany(connection -> {
                    ArrayList<Mono<Integer>> fluxes = new ArrayList<>(threads);
                    Scheduler stress = Schedulers
                            .newBoundedElastic(10, 10, "Stress");
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
                    return Flux.merge(fluxes).last().thenReturn(connection);
                }).blockLast();
        long end = System.nanoTime();
        log.info("Time (s): {}", (end - start) / (1000_000_000.));
        assertNotNull(conn);
        AtomicInteger cc = new AtomicInteger(0);
        long readStart = System.nanoTime();
        assertEquals(
                now + count * threads - 1,
                Mono.from(conn.createStatement("select * from s_test order by id asc")
                        .execute())
                        .flatMapMany(result ->
                                result.map((row, rowMetadata) -> row.get("id", Long.class)))
                        .reduce(now - 1, (previous, current) -> {
                            assertTrue(previous < current);
                            cc.incrementAndGet();
                            return current;
                        }).doOnNext(l -> log.info("Last: {}", l)).block());
        long readEnd = System.nanoTime();
        log.info("Read time (s): {}", (readEnd - readStart) / (1000_000_000.));
        assertEquals(count * threads, cc.get());
    }

    private Mono<Integer> insertIt(Connection conn, long timed, int index) {
        return Mono.from(conn.createStatement("insert into s_test (id, name) " +
                "values (?, ?)")
                .bind(0, timed)
                .bind(1, "Stress-" + index)
                .execute()).flatMap(result ->
                Mono.from(result.getRowsUpdated()).map(l -> (int) (long) l));
    }

    private Mono<JdbcConnection> init(JdbcConnection connection) {
        return Mono.from(connection.createStatement("drop table if exists s_test").execute())
                .thenMany(connection.createStatement("create table s_test" +
                        "(id bigint primary key, name varchar)").execute())
                .last().thenReturn(connection);
    }

    @Test
    void transactionTest() {
        JdbcConnectionFactory factory = (JdbcConnectionFactory)
                ConnectionFactories.get("r2dbc:r2jdbc:h2~:////tmp/test");
        Mono.from(factory.create())
                .flatMap(this::init)
                .flatMapMany(connection -> connection.beginTransaction()
                        .thenMany(Flux.defer(() -> {
                            JdbcStatement statement = connection.createStatement("insert into s_test (id, name)" +
                                    " values (1024, ?)");
                            statement.bind(0, "Hello");
                            Mono<Void> first = statement.execute().flatMap(JdbcResult::getRowsUpdated).then();

                            JdbcStatement second = connection.createStatement("select * from s_test where id = ?");
                            second.bind(0, 1024);
                            Flux<JdbcResult> execute = second.execute();
                            return Flux.merge(
                                    first, execute, connection.commitTransaction()
                            ).log();
                        }))).blockLast();
    }
}
