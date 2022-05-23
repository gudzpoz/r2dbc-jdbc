package party.iroiro.r2jdbc.codecs;

public interface Converter {
    Object convert(Object object, Class<?> target) throws UnsupportedOperationException;
}
