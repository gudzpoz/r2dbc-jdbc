package party.iroiro.r2jdbc;

import io.r2dbc.spi.Statement;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
public class JdbcStatement implements Statement {
    private static final Pattern NAMED_PARAMETER = Pattern.compile("(?:\\s|^):(\\w+)(?:\\s|$)");
    private final JdbcConnection conn;
    private final Map<String, Integer> indices;
    private final AtomicInteger size;
    final AtomicReference<String[]> wantsGenerated;
    final String sql;

    final ArrayList<Map<Integer, Object>> bindings;

    JdbcStatement(String sql, JdbcConnection conn) {
        this.conn = conn;
        bindings = new ArrayList<>();
        add();
        indices = new HashMap<>();
        size = new AtomicInteger(-1);
        wantsGenerated = new AtomicReference<>(null);
        this.sql = simpleParse(sql);
    }

    private String simpleParse(String sql) {
        Matcher matcher = NAMED_PARAMETER.matcher(sql);
        int index = 0;
        while (matcher.find()) {
            indices.put(matcher.group(1), index);
            index++;
        }
        return matcher.replaceAll(" ? ");
    }

    @Override
    public Statement add() {
        bindings.add(new HashMap<>());
        return this;
    }

    private int getIndexOfNamedParameter(String name) {
        if (name == null) {
            throw new IllegalArgumentException("index must not be null");
        }
        Integer integer = indices.get(name);
        if (integer == null) {
            throw new NoSuchElementException(name);
        }
        return integer;
    }

    @Override
    public Statement bind(int index, Object value) {
        //noinspection ConstantConditions
        if (value == null) {
            throw new IllegalArgumentException("value must not be null");
        }
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
    public Mono<JdbcResult> execute() {
        return conn.send(JdbcJob.Job.EXECUTE_STATEMENT, this,
                packet -> new JdbcResult(conn, packet.data, size.get()));
    }

    @Override
    public Statement returnGeneratedValues(String... columns) {
        wantsGenerated.set(columns);
        return this;
    }

    @Override
    public Statement fetchSize(int rows) {
        size.set(rows);
        return this;
    }
}
