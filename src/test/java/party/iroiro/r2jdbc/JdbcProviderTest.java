package party.iroiro.r2jdbc;

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Statement;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class JdbcProviderTest {
    private ConnectionFactory getFactory() {
        return ConnectionFactories.get("r2dbc:r2jdbc:///tmp/test");
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
            insert.bind(1, "JdbcWorker");

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
                            .doOnNext(n -> log.trace("Here it is: {}", n)),
                    Mono.from(connection.close())
            ).log();
        }).blockLast();
    }
}
