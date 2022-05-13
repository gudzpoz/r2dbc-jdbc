package party.iroiro.r2jdbc;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Jdbc Statement wrapper with data processing on a worker thread
 *
 * <p>
 *     Note that named parameter binding has a very limited support, that is,
 *     it is matched with regex <code>(?:\s|^):(\w+)(?:\s|$)</code>, which may work
 *     in most <i>simple</i> cases.
 * </p>
 */
@Slf4j
public class JdbcStatement implements Statement {
    private static final Pattern NAMED_PARAMETER = Pattern.compile("(?:\\s|^):(\\w+)(?:\\s|$)");
    private final JdbcConnection conn;
    private final Map<String, Integer> indices;
    String sql;

    final ArrayList<Map<Integer, Object>> bindings;

    public JdbcStatement(String sql, JdbcConnection conn) {
        this.sql = sql;
        this.conn = conn;
        bindings = new ArrayList<>();
        add();
        indices = new HashMap<>();
    }

    @Override
    public Statement add() {
        bindings.add(new HashMap<>());
        return this;
    }

    private int getIndexOfNamedParameter(String name) {
        if (indices.size() == 0) {
            log.debug("Named Sql: {}", sql);
            Matcher matcher = NAMED_PARAMETER.matcher(sql);
            int index = 0;
            while (matcher.find()) {
                indices.put(matcher.group(1), index);
                index++;
            }
            sql = matcher.replaceAll(" ? ");
            log.debug("Replaced Sql: {}", sql);
            log.debug("Index map: {}", indices);
        }
        Integer integer = indices.get(name);
        if (integer == null) {
            throw new NoSuchElementException(name);
        }
        return integer;
    }

    @Override
    public Statement bind(int index, Object value) {
        bindings.get(bindings.size() - 1).put(index, value);
        return this;
    }

    @Override
    public Statement bind(String name, Object value) {
        return bind(getIndexOfNamedParameter(name), value);
    }

    @Override
    public Statement bindNull(int index, Class<?> type) {
        bindings.get(bindings.size() - 1).put(index, null);
        return this;
    }

    @Override
    public Statement bindNull(String name, Class<?> type) {
        return bindNull(getIndexOfNamedParameter(name), type);
    }

    @Override
    public Publisher<? extends Result> execute() {
        return conn.send(JdbcJob.Job.EXECUTE_STATEMENT, this,
                packet -> new JdbcResult(conn, packet.data)).log();
    }

    @Override
    public Statement returnGeneratedValues(String... columns) {
        // TODO
        return Statement.super.returnGeneratedValues(columns);
    }

    @Override
    public Statement fetchSize(int rows) {
        // TODO
        return Statement.super.fetchSize(rows);
    }
}
