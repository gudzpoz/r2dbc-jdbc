package party.iroiro.r2jdbc;

import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.Result;

public class JdbcException extends R2dbcException implements Result.Message {

    public JdbcException(Exception e) {
        super(e.getMessage(), e);
    }

    public JdbcException(int errorCode) {
        super(null, null, errorCode);
    }

    @Override
    public R2dbcException exception() {
        return this;
    }

    @Override
    public int errorCode() {
        return this.getErrorCode();
    }

    @Override
    public String sqlState() {
        return this.getSqlState();
    }

    @Override
    public String message() {
        return this.getMessage();
    }
}
