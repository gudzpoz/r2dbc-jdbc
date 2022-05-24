package party.iroiro.r2jdbc;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.RowMetadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

public class JdbcRowMetadata implements RowMetadata {
    private final ArrayList<JdbcColumnMetadata> columns;

    JdbcRowMetadata(ArrayList<JdbcColumnMetadata> columns) {
        this.columns = columns;
    }

    @Override
    public ColumnMetadata getColumnMetadata(int index) {
        return columns.get(index);
    }

    public int getColumnIndex(String name) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equalsIgnoreCase(name)) {
                return i;
            }
        }
        throw new NoSuchElementException(name);
    }

    @Override
    public ColumnMetadata getColumnMetadata(String name) {
        int i = getColumnIndex(name);
        if (i >= 0) {
            return columns.get(i);
        } else {
            throw new NoSuchElementException(name);
        }
    }

    @Override
    public List<? extends ColumnMetadata> getColumnMetadatas() {
        return columns;
    }

    @Override
    public Collection<String> getColumnNames() {
        return columns.stream().map(JdbcColumnMetadata::getName).collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return columns.toString();
    }

    @Override
    public boolean contains(String columnName) {
        return getColumnIndex(columnName) >= 0;
    }
}
