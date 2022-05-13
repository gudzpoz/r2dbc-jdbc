package party.iroiro.r2jdbc;

import io.r2dbc.spi.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class JdbcProviderTest {
    private ConnectionFactory getFactory() {
        return ConnectionFactories.get("r2dbc:r2jdbc:h2~:////tmp/test");
    }

    @Test
    public void providerTest() {
        ConnectionFactory factory = getFactory();
        assertEquals(factory.getMetadata().getName(), JdbcConnectionFactoryMetadata.DRIVER_NAME);
    }

    @Test
    public void simpleTest() {
        ConnectionFactory factory = getFactory();

        Mono.from(factory.create()).flatMapMany((connection) -> {
            Batch batch = connection.createBatch();
            batch.add("create table if not exists test (id bigint primary key, name varchar)");

            Statement insert = connection.createStatement("insert into test (id, name) values (?, ?)");
            long now = Instant.now().toEpochMilli();
            insert.bind(0, now);
            String name = "JdbcWorker";
            insert.bind(1, name);

            Statement query = connection.createStatement("select * from test where id = :id");
            query.bind("id", now);

            return Flux.concat(
                    Flux.from(batch.execute())
                            .flatMap(result -> Flux.from(result.getRowsUpdated()).log())
                            .log(),
                    Mono.from(insert.execute())
                            .flatMapMany(result -> Flux.from(result.getRowsUpdated())).log(),
                    Mono.from(query.execute())
                            .flatMapMany(result -> result.map((row, meta) -> row.get("name")))
                            .doOnNext(n -> assertEquals(n, name))
            ).log().then(Mono.just(connection));
        }).delayElements(Duration.ofSeconds(5)).flatMap(connection -> {
            System.gc();
            return connection.close();
        }).blockLast();
    }

    @Test
    public void generatedTest() {
        ConnectionFactory factory = getFactory();

        Flux<? extends Connection> flux = Mono.from(factory.create()).flatMapMany((connection) -> {
            Batch batch = connection.createBatch();
            batch.add("create table if not exists generated_id (name varchar, id identity)");

            Statement insert = connection.createStatement("insert into generated_id (name) values (?)");
            insert.returnGeneratedValues("id");
            String name = "GeneratedId" + UUID.randomUUID();
            insert.bind(0, name);

            Statement query = connection.createStatement("select * from generated_id where id = :id");

            return Flux.concat(
                    Flux.from(batch.execute())
                            .flatMap(result -> Flux.from(result.getRowsUpdated()).log())
                            .log(),
                    Mono.from(insert.execute())
                            .flatMapMany(result -> result.map(((row, rowMetadata) -> {
                                return row.get("id", Long.class);
                            })))
                            .flatMap(id -> {
                                query.bind("id", id);
                                log.trace("Generated Id: {}", id);
                                return query.execute();
                            })
                            .flatMap(result -> result.map((row, meta) -> row.get("name")))
                            .doOnNext(n -> assertEquals(n, name))
            ).log().then(Mono.just(connection));
        });
        flux.delayElements(Duration.ofSeconds(5)).flatMap(connection -> {
            System.gc();
            return connection.close();
        }).blockLast();
    }
}
