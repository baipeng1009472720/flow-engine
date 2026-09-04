package com.playrewards.flowengine.spi;

import java.util.Arrays;
import java.util.Objects;

/**
 * Copying container for secret characters resolved for a short-lived component operation.
 *
 * <p>The value is never rendered by {@link #toString()}. The JVM cannot guarantee immediate clearing
 * of copied character arrays, so callers must not cache or log them. A future {@link SecretResolver}
 * adapter owns the shortest practical secret lifecycle.</p>
 */
public final class SecretValue {

    private final char[] characters;

    /**
     * Copies secret characters so a resolver cannot retain a caller-owned mutable array.
     *
     * @param characters non-null secret characters
     */
    public SecretValue(char[] characters) {
        this.characters = Arrays.copyOf(Objects.requireNonNull(characters, "secret characters must not be null"),
                characters.length);
    }

    /**
     * Returns an independent copy for the immediate downstream API call.
     *
     * @return caller-owned secret character copy
     */
    public char[] copyCharacters() {
        return Arrays.copyOf(characters, characters.length);
    }

    @Override
    public String toString() {
        return "SecretValue[<redacted>]";
    }
}
