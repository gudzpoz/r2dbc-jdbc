/*
 * Copyright 2017-2019 the original author or authors.
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

import io.r2dbc.spi.*;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.PROTOCOL;
import static org.assertj.core.api.Assertions.assertThat;
import static party.iroiro.r2jdbc.JdbcConnectionFactoryProvider.URL;

final class H2ConnectionFactoryProviderTest {

    private final JdbcConnectionFactoryProvider provider = new JdbcConnectionFactoryProvider();

    @Test
    void doesNotSupportWithWrongDriver() {
        assertThat(this.provider.supports(ConnectionFactoryOptions.builder()
            .option(DRIVER, "test-driver")
            .option(URL, "test-url")
            .build())).isFalse();
    }

    @Test
    void doesNotSupportWithoutDriver() {
        assertThat(this.provider.supports(ConnectionFactoryOptions.builder()
            .option(URL, "test-url")
            .build())).isFalse();
    }

    @Test
    void supportsWithProtocolAndDatabase() {
        assertThat(this.provider.supports(ConnectionFactoryOptions.builder()
            .option(DRIVER, JdbcConnectionFactoryMetadata.DRIVER_NAME)
            .option(PROTOCOL, "h2~mem")
            .option(DATABASE, "test-database")
            .build())).isTrue();
    }

    @Test
    void supportsWithUrl() {
        assertThat(this.provider.supports(ConnectionFactoryOptions.builder()
            .option(DRIVER, JdbcConnectionFactoryMetadata.DRIVER_NAME)
            .option(URL, "h2:tcp://localhost:8080//tmp/test")
            .build())).isTrue();
    }

    @Test
    void supportsKnownOptions() {
        ConnectionFactory connectionFactory = ConnectionFactories.get(
                "r2dbc:r2jdbc:h2~mem:///option-test?access_mode_data=r&j2forward=access_mode_data");
        Mono.from(connectionFactory.create())
            .flatMapMany(o -> o.createStatement("CREATE TABLE option_test (id int)").execute())
            .as(StepVerifier::create)
            .expectErrorSatisfies(e ->
                    assertThat(e).isInstanceOf(R2dbcException.class).hasMessageContaining("90097")).verify();
    }

    @Test
    void returnsDriverIdentifier() {
        assertThat(this.provider.getDriver()).isEqualTo(JdbcConnectionFactoryMetadata.DRIVER_NAME);
    }
}
