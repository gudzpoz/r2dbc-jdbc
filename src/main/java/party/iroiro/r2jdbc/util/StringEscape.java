package party.iroiro.r2jdbc.util;

import java.util.function.UnaryOperator;

public class StringEscape {
    private final char start;
    private final UnaryOperator<Character> replacer;

    public StringEscape(char start, UnaryOperator<Character> replacer) {
        this.start = start;
        this.replacer = replacer;
    }

    public String unescape(String input) {
        StringBuilder builder = new StringBuilder(input.length());
        boolean escaping = false;
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            if (escaping) {
                builder.append(replacer.apply(c));
                escaping = false;
            } else {
                if (c == start) {
                    escaping = true;
                } else {
                    builder.append(c);
                }
            }
        }
        return builder.toString();
    }
}
