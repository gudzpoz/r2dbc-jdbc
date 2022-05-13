package party.iroiro.r2jdbc;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import lombok.Data;

import java.util.ArrayList;

@Data
public class JdbcRow implements Row, Result.RowSegment {
    private final ArrayList<Object> rowData;
    private JdbcRowMetadata metadata;

    JdbcRow(ArrayList<Object> rowData) {
        this.rowData = rowData;
    }

    @Override
    public <T> T get(int index, Class<T> type) {
        return type.cast(rowData.get(index));
    }

    @Override
    public <T> T get(String name, Class<T> type) {
        return type.cast(rowData.get(metadata.getColumnIndex(name)));
    }

    public String toString() {
        StringBuilder builder = new StringBuilder("[\n");
        for (Object object : rowData) {
            builder
                    .append("  ")
                    .append(object.getClass().getName())
                    .append(": ")
                    .append(object)
                    .append(",\n");
        }
        builder.append("]");
        return builder.toString();
    }

    @Override
    public Row row() {
        return this;
    }
}
