package party.iroiro.r2jdbc;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import lombok.NonNull;
import party.iroiro.r2jdbc.codecs.Converter;

import java.util.ArrayList;
import java.util.NoSuchElementException;

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
            return type.cast(converter.decode(rowData.get(index), type));
        }
    }

    @Override
    public <T> T get(String name, Class<T> type) {
        int columnIndex = metadata.getColumnIndex(name);
        if (columnIndex == -1) {
            throw new NoSuchElementException(name);
        } else {
            return get(columnIndex, type);
        }
    }

    public void setConverter(Converter converter) {
        this.converter = converter;
    }

    @Override
    public Row row() {
        return this;
    }

    @Override
    @NonNull
    public RowMetadata getMetadata() {
        if (metadata == null) {
            throw new NullPointerException();
        } else {
            return metadata;
        }
    }

    public void setMetadata(JdbcRowMetadata metadata) {
        this.metadata = metadata;
    }
}
