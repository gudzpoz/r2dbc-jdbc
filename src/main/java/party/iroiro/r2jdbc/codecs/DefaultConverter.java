package party.iroiro.r2jdbc.codecs;

import org.apache.commons.beanutils.ConversionException;
import org.apache.commons.beanutils.ConvertUtilsBean;

public class DefaultConverter implements Converter {
    protected final ConvertUtilsBean converter;

    public DefaultConverter() {
        converter = new ConvertUtilsBean();
    }

    @Override
    public Object convert(Object object, Class<?> target) throws UnsupportedOperationException {
        try {
            return converter.convert(object, target);
        } catch (ConversionException e) {
            throw new UnsupportedOperationException(e);
        }
    }
}
