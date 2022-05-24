# Reactive Relational Database Connectivity JDBC Implementation

![Maven Central](https://img.shields.io/maven-central/v/party.iroiro/r2dbc-jdbc?label=Maven)
![Sonatype Nexus (Snapshots)](https://img.shields.io/nexus/s/party.iroiro/r2dbc-jdbc?server=https%3A%2F%2Fs01.oss.sonatype.org&label=Nexus)
[![GitHub](https://img.shields.io/github/license/gudzpoz/r2dbc-jdbc?label=License)](./LICENSE)

[![Build and Publish](https://github.com/gudzpoz/r2dbc-jdbc/actions/workflows/build.yml/badge.svg)](https://github.com/gudzpoz/r2dbc-jdbc/actions/workflows/build.yml)
[![Test](https://github.com/gudzpoz/r2dbc-jdbc/actions/workflows/test.yml/badge.svg)](https://github.com/gudzpoz/r2dbc-jdbc/actions/workflows/test.yml)
[![Code Coverage](https://img.shields.io/codecov/c/gh/gudzpoz/r2dbc-jdbc?label=Test%20Coverage)](https://app.codecov.io/gh/gudzpoz/r2dbc-jdbc)

This project contains a simplistic [Java Database Connectivity (JDBC)](https://docs.oracle.com/javase/8/docs/technotes/guides/jdbc/) implementation of the [R2DBC SPI](https://github.com/r2dbc/r2dbc-spi). This implementation is not intended to be used directly, but rather to be used as the backing implementation for a humane client library.

It requires Java 9.

## Status

It is a toy project, and I promise there will be bugs.

## Wait, what?

```text
JDBC Connection                                 Reactive
     /|\                                         Access
      |                                             |
|------------|  Input Jobs (BlockingQueue<>)       \|/
|    The     |  <--------------------------- Connection(s)
|            |
|  worker(s) |  --------------------------->   Dispatcher
|------------|  (LinkedBlockingMultiQueue<>)        |
                           Output                   |
                                                   \|/
              Executing callbacks in Schedulers.parallel()
```

## Why?

[R2DBC H2](https://github.com/r2dbc/r2dbc-h2) is not *that* non-blocking. This library, as is shown in the above diagram, actually simulates the regular TCP Client-Server connection to provider reactive access.

For *big* databases like MariaDB or PostgreSQL, you might as well go to [Drivers](https://r2dbc.io/drivers/) to find a better SPI. However, if you want reactivity for some embedded databases like [H2](https://www.h2database.com), [Derby](https://db.apache.org/derby/) or [HSQLDB](https://hsqldb.org/), maybe you can have a go with this.

## Usage

Just follow the guide on [R2DBC](https://r2dbc.io/).

Note that you cannot pass a raw JDBC url directly. With R2DBC API, the call might look like this:

```java
import io.r2dbc.spi.ConnectionFactories;

class Main {
    void test() {
        // String
        ConnectionFactories.get(
                "r2dbc:r2jdbc:h2~mem:///test"
        );
        // ConnectionFactoryOptions is more friendly
        ConnectionFactories.get(
                ConnectionFactoryOptions.builder()
                        .option(ConnectionFactoryOptions.PROTOCOL, "r2dbc")
                        .option(ConnectionFactoryOptions.DATABASE, "r2jdbc")
                        .option(JdbcConnectionFactoryProvider.URL, "jdbc:h2:mem:test")
                        .build());
    }
}
```

We accept four extra options. All the four are optional. 

<table>
<tr><th>Option</th><th>Explained</th></tr>
<tr><td>

`JdbcConnectionFactoryProvider.URL`

Example: `r2dbc:r2jdbc:h2:///?j2url=jdbc:h2:mem:test`</td>
<td>Used directly as JDBC url

Default: **Deduce from R2DBC url**</td>
</tr><tr><td>

`JdbcConnectionFactoryProvider.SHARED`

Example: `r2dbc:r2jdbc:h2~mem:///test?j2shared=true`</td>
<td>Multiple connection shares the same worker.

Default: **No**</td>
</tr><tr><td>

`JdbcConnectionFactoryProvider.WAIT`

Example: `r2dbc:r2jdbc:h2~mem:///test?j2wait=-1`</td>
<td>If sharing workers, wait for WAIT (ms) interval for new connections before shutting down worker when all connections closes. If negative, the JDBC connection is only closed after closing the factory.

Default: **Do not wait. Shut down once all connection closes.**</td>
</tr><tr><td>

`JdbcConnectionFactoryProvider.FORWARD`

Example: `r2dbc:r2jdbc:h2:///test?CIPHER=AES&j2forward=CIPHER`</td>
<td>What R2DBC options to forward to JDBC (comma separated).

Default: **None**</td>
</tr><tr><td>

`JdbcConnectionFactoryProvider.CODEC`

Example: `r2dbc:r2jdbc:h2:///test?j2codec=party.iroiro.r2jdbc.codecs.DefaultCodec`</td>
<td>Name of a class converting JDBC types into regular Java types (used by the worker).

Default: **Built-in**</td>
</tr><tr>
<td>

`JdbcConnectionFactoryProvider.CONV`


Example: `r2dbc:r2jdbc:h2:///test?j2conv=party.iroiro.r2jdbc.codecs.DefaultConverter`</td>
<td>Name of a class converting between Java types.

Default: **Built-in**</td>
</tr>
</table>

## License

I am borrowing tests from [R2DBC H2](https://github.com/r2dbc/r2dbc-h2), which is licensed under [Apache License 2.0](https://github.com/r2dbc/r2dbc-h2/blob/main/LICENSE).

Licensed under the [Apache License Version 2.0](./LICENSE)
