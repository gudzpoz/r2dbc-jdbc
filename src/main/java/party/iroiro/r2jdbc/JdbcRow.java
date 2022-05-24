package party.iroiro.r2jdbc;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import party.iroiro.r2jdbc.codecs.Converter;

import java.util.ArrayList;

public class JdbcRow implements Row, Result.RowSegment {
    private final ArrayList<Object> rowData;
    private JdbcRowMetadata metadata;
    private Converter converter;

    JdbcRow(ArrayList<Object> rowData) {
        this.rowData = rowData;
    }

    @Override
    public <T> T get(int index, Class<T> type) {
        if (converter == null) {
            return type.cast(rowData.get(index));
        } else {
            return type.cast(converter.convert(rowData.get(index), type));
        }
    }

    @Override
    public <T> T get(String name, Class<T> type) {
        return get(metadata.getColumnIndex(name), type);
    }

    public void setConverter(Converter converter) {
        this.converter = converter;
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

    @Override
    public RowMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(JdbcRowMetadata metadata) {
        this.metadata = metadata;
    }
}
