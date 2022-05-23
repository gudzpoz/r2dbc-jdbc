package party.iroiro.r2jdbc;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.jdbc.core.JdbcOperations;
import party.iroiro.r2jdbc.util.JdbcServerExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Collections;

import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static party.iroiro.r2jdbc.JdbcConnectionFactoryProvider.URL;

public class H2RowTest {
    @RegisterExtension
    static final JdbcServerExtension SERVER = new JdbcServerExtension();

    private final ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
            .option(DRIVER, JdbcConnectionFactoryMetadata.DRIVER_NAME)
            .option(URL, SERVER.getUrl())
            .build());

    static <T> Mono<T> close(Connection connection) {
        return Mono.from(connection
                .close())
                .then(Mono.empty());
    }

    @BeforeEach
    void createTable() {
        getJdbcOperations().execute("CREATE TABLE test ( test_value INTEGER )");
    }

    @AfterEach
    void dropTable() {
        getJdbcOperations().execute("DROP TABLE test");
    }

    @Test
    void selectWithAliases() {
        getJdbcOperations().execute("INSERT INTO test VALUES (100)");

        Mono.from(this.connectionFactory.create())
                .flatMapMany(connection -> Flux.from(connection

                        .createStatement("SELECT test_value as ALIASED_VALUE FROM test")
                        .execute())
                        .flatMap(result -> Flux.from(result
                                .map((row, rowMetadata) -> row.get("ALIASED_VALUE", Integer.class)))
                                .collectList())

                        .concatWith(close(connection)))
                .as(StepVerifier::create)
                .expectNext(Collections.singletonList(100))
                .verifyComplete();
    }

    @Test
    void selectWithoutAliases() {
        getJdbcOperations().execute("INSERT INTO test VALUES (100)");

        Mono.from(this.connectionFactory.create())
                .flatMapMany(connection -> Flux.from(connection

                        .createStatement("SELECT test_value FROM test")
                        .execute())
                        .flatMap(new JdbcTestKit()::extractColumns)

                        .concatWith(close(connection)))
                .as(StepVerifier::create)
                .expectNextMatches(integers -> {
                    assertIterableEquals(Collections.singletonList(100), integers);
                    return true;
                })
                .verifyComplete();
    }

    private JdbcOperations getJdbcOperations() {
        JdbcOperations jdbcOperations = SERVER.getJdbcOperations();

        if (jdbcOperations == null) {
            throw new IllegalStateException("JdbcOperations not yet initialized");
        }

        return jdbcOperations;
    }
}
