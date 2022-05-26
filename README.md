# Reactive Relational Database Connectivity JDBC Implementation

[![Maven Central](https://img.shields.io/maven-central/v/party.iroiro/r2dbc-jdbc?label=Maven%20Central&color=blue)](https://mvnrepository.com/artifact/party.iroiro/r2dbc-jdbc)
![Sonatype Nexus (Snapshots)](https://img.shields.io/nexus/s/party.iroiro/r2dbc-jdbc?server=https%3A%2F%2Fs01.oss.sonatype.org&label=Nexus&color=pink)
[![GitHub](https://img.shields.io/github/license/gudzpoz/r2dbc-jdbc?label=License)](./LICENSE)

[![Build and Publish](https://github.com/gudzpoz/r2dbc-jdbc/actions/workflows/build.yml/badge.svg)](https://github.com/gudzpoz/r2dbc-jdbc/actions/workflows/build.yml)
[![Test](https://github.com/gudzpoz/r2dbc-jdbc/actions/workflows/test.yml/badge.svg)](https://github.com/gudzpoz/r2dbc-jdbc/actions/workflows/test.yml)
[![Code Coverage](https://img.shields.io/codecov/c/gh/gudzpoz/r2dbc-jdbc?label=Test%20Coverage)](https://app.codecov.io/gh/gudzpoz/r2dbc-jdbc)

This project contains a simplistic [Java Database Connectivity (JDBC)](https://docs.oracle.com/javase/8/docs/technotes/guides/jdbc/)
implementation of the [R2DBC SPI](https://github.com/r2dbc/r2dbc-spi). This implementation is not intended to be used directly,
but rather to be used as the backing implementation for a humane client library.

As for Spring Data, this library provides a `R2dbcDialectProvider`.
You may simply set `spring.r2dbc.url` accordingly in your `application.yml/properties`
and things should work out of the box.

It requires Java 9.

## Status

It is a toy project, and I promise there will be bugs.

## Wait, what?

```text
JDBC Connections                                Reactive
     /|\                                         Access
      |                                             |
|------------|  Input Jobs (BlockingQueue<>)       \|/
|    The     |  <--------------------------- R2dbcConnections
|            |
|  worker(s) |  --------------------------->   Dispatcher
|------------|  (LinkedBlockingMultiQueue<>)        |
                           Output                   |
                                                   \|/
              Executing callbacks in Schedulers.parallel()
```

## Why?

[R2DBC H2](https://github.com/r2dbc/r2dbc-h2) is not *that* non-blocking. This library, as is shown in the above diagram,
actually simulates the regular TCP Client-Server connection to provider reactive access.

For *big* databases like MariaDB or PostgreSQL, you might as well go to [Drivers](https://r2dbc.io/drivers/) to find a
better SPI. However, if you want reactivity for some embedded databases like [H2](https://www.h2database.com),
[Derby](https://db.apache.org/derby/) or [HSQLDB](https://hsqldb.org/), maybe you can have a go with this.

## Usage

Just follow the guide on [R2DBC](https://r2dbc.io/).

### From Maven Central

[![Maven Central](https://img.shields.io/maven-central/v/party.iroiro/r2dbc-jdbc?label=Maven%20Central&color=blue)](https://mvnrepository.com/artifact/party.iroiro/r2dbc-jdbc)

<details>
<summary>Maven</summary>

Using directly?

```xml
<dependency>
  <groupId>party.iroiro</groupId>
  <artifactId>r2dbc-jdbc</artifactId>
  <version>0.2.0</version>
</dependency>
```

</details>

<details>
<summary>Gradle</summary>

Using directly?

```groovy
implementation 'party.iroiro:r2dbc-jdbc:0.2.0'
```

</details>

### Use with Spring Data

<details>
<summary>Maven</summary>

```xml
<dependency>
  <groupId>org.springframework.boot</groupId>
  <artifactId>spring-boot-starter-data-r2dbc</artifactId>
</dependency>
<dependency>
  <groupId>party.iroiro</groupId>
  <artifactId>r2dbc-jdbc</artifactId>
  <version>0.2.0</version>
  <scope>runtime</scope>
</dependency>
```

</details>

<details>
<summary>Gradle</summary>

```groovy
implementation 'org.springframework.boot:spring-boot-starter-data-r2dbc'
runtimeOnly 'party.iroiro:r2dbc-jdbc:0.2.0'
```

</details>

Spring Data requires a `R2dbcDialectProvider` to map repository operations to SQLs.
Since different databases may differ in SQL grammar, you may set one matching your database with,
for example:

```java
System.setProperty("j2dialect", "org.springframework.data.r2dbc.dialect.H2Dialect");
```

Currently, available dialects in Spring Data Reactive are:

- `org.springframework.data.r2dbc.dialect.H2Dialect`
- `org.springframework.data.r2dbc.dialect.PostgresDialect`
- `org.springframework.data.r2dbc.dialect.MySqlDialect`
- `org.springframework.data.r2dbc.dialect.OracleDialect`
- `org.springframework.data.r2dbc.dialect.SqlServerDialect`

### Passing JDBC urls

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

We accept some extra options. All are optional. 

<table>
<tr><th>Option</th><th>Explained</th></tr>
<tr><td>

`JdbcConnectionFactoryProvider.URL`

Example: `r2dbc:r2jdbc:h2:///?j2url=jdbc:h2:mem:test`</td>
<td>Used directly as JDBC url

Default: **Infer from R2DBC url**</td>
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

I am borrowing tests from [R2DBC H2](https://github.com/r2dbc/r2dbc-h2), which is licensed under
[Apache License 2.0](https://github.com/r2dbc/r2dbc-h2/blob/main/LICENSE).

Licensed under the [Apache License Version 2.0](./LICENSE)
