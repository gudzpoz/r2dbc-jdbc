/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package party.iroiro.r2jdbc;

import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Result;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

final class H2ConnectionFactoryInMemoryTest {

    @Test
    void shouldCreateInMemoryDatabase() {

        JdbcConnectionFactory connectionFactory = new JdbcConnectionFactory(
                ConnectionFactoryOptions.parse("r2dbc:r2jdbc:h2~mem:///" + UUID.randomUUID()));

        connectionFactory.create().flatMapMany(JdbcConnection::close)
                .as(StepVerifier::create).verifyComplete();
    }

    @Test
    void retainsStateAfterRunningCommand() {

        JdbcConnectionFactory connectionFactory = new JdbcConnectionFactory(
                ConnectionFactoryOptions.parse("r2dbc:r2jdbc:h2~mem:///" + UUID.randomUUID()
                        + "?j2shared=1&j2wait=1000"));

        runCommand(connectionFactory, "CREATE TABLE lego (id INT);");
        runCommand(connectionFactory, "INSERT INTO lego VALUES(42);");
    }

    @Test
    void databaseClosedAfterFactoryClose() {

        String database = UUID.randomUUID().toString();
        JdbcConnectionFactory connectionFactory = new JdbcConnectionFactory(
                ConnectionFactoryOptions.parse("r2dbc:r2jdbc:h2~mem:///" + database + "?j2shared=1"));

        runCommand(connectionFactory, "CREATE TABLE lego (id INT);");

        connectionFactory.close().as(StepVerifier::create).verifyComplete();

        JdbcConnectionFactory nextInstance = new JdbcConnectionFactory(
                ConnectionFactoryOptions.parse("r2dbc:r2jdbc:h2~mem:///" + database + "?j2shared=1"));
        runCommand(nextInstance, "CREATE TABLE lego (id INT);");
    }

    @Test
    void closedDatabaseFailsToCreateConnections() {
        String database = UUID.randomUUID().toString();
        JdbcConnectionFactory connectionFactory = new JdbcConnectionFactory(
                ConnectionFactoryOptions.parse("r2dbc:r2jdbc:h2~mem:///" + database));

        runCommand(connectionFactory, "CREATE TABLE lego (id INT);");

        connectionFactory.close().as(StepVerifier::create).verifyComplete();
        connectionFactory.create().as(StepVerifier::create).verifyError(IllegalThreadStateException.class);
    }

    @Test
    void automaticallyClosesAfterNoConnections() throws InterruptedException {

        String database = UUID.randomUUID().toString();
        JdbcConnectionFactory connectionFactory = new JdbcConnectionFactory(
                ConnectionFactoryOptions.parse(
                        "r2dbc:r2jdbc:h2~mem:///" + database + "?j2shared=1&j2wait=500"));

        runCommand(connectionFactory, "CREATE TABLE lego (id INT);");
        JdbcConnection userSession = connectionFactory.create().block();
        assertNotNull(userSession);
        JdbcWorker worker = userSession.getWorker();
        assertNotNull(worker);
        userSession.close().as(StepVerifier::create).verifyComplete();

        assertTrue(worker.isAlive());
        Thread.sleep(100);
        assertTrue(worker.isAlive());
        Thread.sleep(600);
        assertFalse(worker.isAlive());
    }

    @Test
    void closedDatabaseShutsDownClientSessions() {

        String database = UUID.randomUUID().toString();
        JdbcConnectionFactory connectionFactory = new JdbcConnectionFactory(
                ConnectionFactoryOptions.parse("r2dbc:r2jdbc:h2~mem:///" + database + "?j2shared=1"));

        JdbcConnection userSession = connectionFactory.create().block();

        assertNotNull(userSession);

        connectionFactory.close().as(StepVerifier::create).verifyComplete();

        userSession.createStatement("CREATE TABLE lego (id INT);")
                .execute().as(StepVerifier::create)
                .verifyError(JdbcException.class);
    }

    static void runCommand(ConnectionFactory connectionFactory, String sql) {
        Mono.from(connectionFactory.create()).flatMapMany(it ->
                Flux.from(it.createStatement(sql).execute())
                        .flatMap(Result::getRowsUpdated).thenMany(it.close())).as(StepVerifier::create)
            .verifyComplete();
    }
}
