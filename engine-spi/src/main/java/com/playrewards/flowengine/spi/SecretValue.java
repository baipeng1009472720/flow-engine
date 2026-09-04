package com.playrewards.flowengine.spi;

import java.util.Arrays;
import java.util.Objects;

/**
 * Copying container for secret characters resolved for a short-lived component operation.
 *
 * <p>The value is never rendered by {@link #toString()}. Callers receive copies and should clear them
 * promptly; {@link #destroy()} clears only this container's internal buffer and is safe to call repeatedly.</p>
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

    /** Clears this instance's retained characters after use. */
    public void destroy() {
        Arrays.fill(characters, '\0');
    }

    @Override
    public String toString() {
        return "SecretValue[<redacted>]";
    }
}
