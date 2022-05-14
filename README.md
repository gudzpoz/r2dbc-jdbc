# Reactive Relational Database Connectivity JDBC Implementation

This project contains a simplistic [Java Database Connectivity (JDBC)](https://docs.oracle.com/javase/8/docs/technotes/guides/jdbc/) implementation of the [R2DBC SPI](https://github.com/r2dbc/r2dbc-spi). This implementation is not intended to be used directly, but rather to be used as the backing implementation for a humane client library.

## Status

It is a toy project, and I promise there will be bugs.

Multiple connections sharing a single worker requires locking quite a lot, but I have not yet found a good way to handle locks across reactive callbacks. That means, yes, I *expect* bugs when connections create and close too quickly.

## Wait, what?

```text
|------------|  Input Jobs (BlockingQueue<>)
|            |  <--------------------------- Connection(s)
| The worker |
|            |  --------------------------->   Dispatcher
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

| Option | Explained | 
| -------- | --------- |
| `JdbcConnectionFactoryProvider.URL` | Used directly as JDBC url<br>Default: **Deduce from R2DBC url** |
| `JdbcConnectionFactoryProvider.SHARED` | Multiple connection shares the same worker. <br>Default: **No** |
| `JdbcConnectionFactoryProvider.WAIT` | If sharing workers, wait for WAIT (ms) interval for new connections before shutting down worker when all connections closes.<br>Default: **Do not wait. Shut down once all connection closes.** |
| `JdbcConnectionFactoryProvider.FORWARD` | What R2DBC options to forward to JDBC (comma separated).<br>Default: **None** |

## License

I am borrowing tests from [R2DBC H2](https://github.com/r2dbc/r2dbc-h2), which is licensed under [Apache License 2.0](https://github.com/r2dbc/r2dbc-h2/blob/main/LICENSE).

You must wait until I decide on a license for this project.
