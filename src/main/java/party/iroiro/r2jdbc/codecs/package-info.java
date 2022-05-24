/**
 * See {@link party.iroiro.r2jdbc.codecs.Codec} and {@link party.iroiro.r2jdbc.codecs.Converter}
 *
 * <p>
 * To customize conversions (possibly due to bugs in this library or lack of support in JDBC drivers),
 * you need to implement {@link party.iroiro.r2jdbc.codecs.Codec} and/or {@link party.iroiro.r2jdbc.codecs.Converter},
 * probably by extending {@link party.iroiro.r2jdbc.codecs.DefaultCodec} or
 * {@link party.iroiro.r2jdbc.codecs.DefaultConverter}.
 * </p>
 *
 * <p>
 * To use it, you need to add the implementation classes to
 * {@link party.iroiro.r2jdbc.JdbcConnectionFactoryProvider#CODEC} and/or
 * {@link party.iroiro.r2jdbc.JdbcConnectionFactoryProvider#CONV} in your R2DBC urls.
 * </p>
 */
@NonNullApi
package party.iroiro.r2jdbc.codecs;

import reactor.util.annotation.NonNullApi;