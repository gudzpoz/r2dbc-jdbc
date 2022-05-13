/*
 * Copyright 2018 the original author or authors.
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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.sql.SQLException;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.*;

@SuppressWarnings("ConstantConditions")
final class H2BatchTest {

    private static final JdbcConnection connection = mock(JdbcConnection.class, RETURNS_SMART_NULLS);

    @Test
    void addNoSql() {
        assertThatIllegalArgumentException().isThrownBy(() -> new JdbcBatch(connection).add(null))
            .withMessage("sql must not be null");
    }

    @Disabled("Not yet implemented")
    @Test
    void addWithParameter() {
        assertThatIllegalArgumentException().isThrownBy(() -> new JdbcBatch(connection).add("test-query-$1"))
            .withMessage("Statement 'test-query-$1' is not supported.  This is often due to the presence of parameters.");
    }

    @Test
    void constructorNoClient() {
        assertThatIllegalArgumentException().isThrownBy(() -> new JdbcBatch(null))
            .withMessage("connection must not be null");
    }

    @Test
    void execute() {
        when(connection.send(eq(JdbcJob.Job.BATCH), any(), any()))
                .thenAnswer(invocation -> {
                    JdbcBatch argument = invocation.getArgument(1);
                    return Mono.just(argument.sql.stream().map(ignored -> 1).collect(Collectors.toList()));
                });

        new JdbcBatch(connection)
            .add("select test-query-1")
            .add("select test-query-2")
            .execute()
            .as(StepVerifier::create)
            .expectNextCount(2)
            .verifyComplete();
    }

    @Test
    void executeErrorResponse() {
        when(connection.send(eq(JdbcJob.Job.BATCH), any(), any()))
                .thenAnswer(invocation -> Mono.error(new SQLException()));

        new JdbcBatch(connection)
            .add("select test-query")
            .execute()
            .as(StepVerifier::create)
            .verifyError(SQLException.class);
    }

}
