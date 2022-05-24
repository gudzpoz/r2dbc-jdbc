package party.iroiro.r2jdbc.util;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class Pair {
    private final Object first;
    private final Object second;

    public Object getFirst() {
        return first;
    }

    public Object getSecond() {
        return second;
    }
}
