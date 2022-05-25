package party.iroiro.r2jdbc;

import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.Statement;
import reactor.core.publisher.Flux;
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
 * Note that named parameter binding has a very limited support, that is,
 * it is matched with regex <code>(?:\s|^):(\w+)(?:\s|$)</code>, which may work
 * in most <i>simple</i> cases.
 * </p>
 */
public class JdbcStatement implements Statement {
    private static final Pattern NAMED_PARAMETER = Pattern.compile("(?:\\s|^):(\\w+)(?:\\s|$)");
    final AtomicReference<String[]> wantsGenerated;
    final String sql;
    final ArrayList<Map<Integer, Mono<Object>>> rawBindings;
    final ArrayList<Map<Integer, Object>> bindings;
    private final JdbcConnection conn;
    private final Map<String, Integer> indices;
    private final AtomicInteger size;
    private final int maxParameterCount;

    JdbcStatement(String sql, JdbcConnection conn) {
        this.conn = conn;
        bindings = new ArrayList<>();
        rawBindings = new ArrayList<>();
        add();
        indices = new HashMap<>();
        size = new AtomicInteger(-1);
        wantsGenerated = new AtomicReference<>(null);
        this.sql = simpleParse(sql);
        maxParameterCount = (int) this.sql.chars().filter(i -> i == '?').count();
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
        rawBindings.add(new HashMap<>());
        return this;
    }

    private int getIndexOfNamedParameter(String name) {
        //noinspection ConstantConditions
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
        if (value instanceof Class) {
            throw new IllegalArgumentException("probably no databases support this");
        }
        if (index >= maxParameterCount) {
            throw new IndexOutOfBoundsException("non existent index");
        }
        rawBindings.get(rawBindings.size() - 1).put(index, conn.getConverter().encode(value));
        return this;
    }

    @Override
    public Statement bind(String name, Object value) {
        return bind(getIndexOfNamedParameter(name), value);
    }

    @Override
    public Statement bindNull(int index, Class<?> type) {
        rawBindings.get(rawBindings.size() - 1).put(index, Mono.just(Parameters.in(type)));
        return this;
    }

    @Override
    public Statement bindNull(String name, Class<?> type) {
        return bindNull(getIndexOfNamedParameter(name), type);
    }

    @Override
    public Flux<JdbcResult> execute() {
        bindings.clear();
        bindings.ensureCapacity(rawBindings.size());
        ArrayList<Flux<Object>> fluxes = new ArrayList<>(rawBindings.size());
        for (Map<Integer, Mono<Object>> rawBinding : rawBindings) {
            final HashMap<Integer, Object> bind = new HashMap<>();
            bindings.add(bind);
            fluxes.add(Flux.fromIterable(rawBinding.entrySet())
                    .flatMap(entry -> {
                        Mono<Object> value = entry.getValue();
                        if (value == null) {
                            return Mono.empty();
                        } else {
                            return value.doOnNext(o -> bind.put(entry.getKey(), o));
                        }
                    }));
        }
        return Flux.merge(fluxes).thenMany(
                conn.send(JdbcJob.Job.EXECUTE_STATEMENT,
                        this,
                        packet -> (ArrayList<?>) packet.data)
                        .flatMapMany(list ->
                                Flux.fromIterable(list)
                                        .map(item -> new JdbcResult(conn, item, conn.getConverter()))));
    }

    @Override
    public Statement returnGeneratedValues(String... columns) {
        //noinspection ConstantConditions
        if (columns == null) {
            throw new IllegalArgumentException();
        }
        wantsGenerated.set(columns);
        return this;
    }

    @Override
    public Statement fetchSize(int rows) {
        size.set(rows);
        return this;
    }
}
