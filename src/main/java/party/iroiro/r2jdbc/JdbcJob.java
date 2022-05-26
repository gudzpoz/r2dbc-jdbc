package party.iroiro.r2jdbc;

import lombok.AllArgsConstructor;

import java.sql.Connection;
import java.util.function.BiConsumer;

@AllArgsConstructor
public class JdbcJob {
    enum Job {
        INIT_CONNECTION,
        CLOSE_CONNECTION,

        CLOSE,

        START_TRANSACTION,
        END_TRANSACTION,
        ROLLBACK_TRANSACTION,
        SET_ISOLATION_LEVEL,

        GET_AUTO_COMMIT,
        SET_AUTO_COMMIT,

        VALIDATE,

        EXECUTE_STATEMENT,

        RESULT_METADATA,
        RESULT_ROWS,
        CLOSE_RESULT,

        BATCH,
    }
    public final Connection connection;
    public final Job job;
    public final Object data;
    public final BiConsumer<JdbcPacket, Throwable> consumer;
}
