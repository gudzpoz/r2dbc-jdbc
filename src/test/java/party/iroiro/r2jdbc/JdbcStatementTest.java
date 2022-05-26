package party.iroiro.r2jdbc;

import org.junit.jupiter.api.Test;
import party.iroiro.r2jdbc.codecs.DefaultConverter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JdbcStatementTest {
    @Test
    public void statementTest() {
        JdbcConnection mock = mock(JdbcConnection.class);
        when(mock.getConverter()).thenReturn(new DefaultConverter());
        JdbcStatement statement = new JdbcStatement("select * from test where id = :id", mock);
        assertEquals(statement, statement.bind("id", 11));
        assertEquals(statement, statement.bind(0, 11));
        assertEquals(statement, statement.bindNull(0, Integer.class));
        assertEquals(statement, statement.bindNull("id", Integer.class));
    }
}
