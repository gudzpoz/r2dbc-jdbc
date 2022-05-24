package party.iroiro.r2jdbc;

import org.h2.api.ErrorCode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ExceptionTest {
    @Test
    public void jdbcException() {
        JdbcException exception = new JdbcException(ErrorCode.ERROR_OPENING_DATABASE_1);
        assertNull(exception.message());
        assertNull(exception.sqlState());
        assertEquals(exception, exception.exception());
        assertEquals(ErrorCode.ERROR_OPENING_DATABASE_1, exception.errorCode());
    }
}
