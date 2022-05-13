/*
 * Copyright 2018-2019 the original author or authors.
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

import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;
import java.nio.file.Files;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

/**
 * Support class for integration tests.
 */
public abstract class IntegrationTestSupport {

    protected static JdbcConnection connection;
    protected static JdbcConnectionFactory connectionFactory;

    @BeforeAll
    static void beforeAll() throws IOException {

        ConnectionFactoryOptions options = builder()
                .option(DRIVER, JdbcConnectionFactoryMetadata.DRIVER_NAME)
                .option(PASSWORD, "test")
                .option(USER, "test")
                .option(PROTOCOL, "h2~")
                .option(DATABASE, Files.createTempDirectory("h2test")
                        .toAbsolutePath().resolve("test").toString())
                .build();

        connectionFactory =
                (JdbcConnectionFactory) ConnectionFactories.get(options);
        connection = connectionFactory.create().block();
    }

    @AfterAll
    static void afterAll() {
        if (connection != null) {
            connection.close().subscribe();
        }
    }
}
