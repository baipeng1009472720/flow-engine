package com.playrewards.flowengine.contract.json;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Creates immutable copies of the JSON value subset accepted by runtime contracts.
 *
 * <p>The boundary intentionally accepts only JSON-compatible maps, arrays, scalars and {@code null};
 * arbitrary Java objects must be converted by an adapter before they enter a contract. This prevents a
 * mutable host object from changing a bundle after it has been compiled or hashed.</p>
 */
public final class JsonValues {

    private static final long MAX_EXACT_NUMBER_WORKSPACE_BYTES = 4_194_304;
    private static final long NUMBER_ESTIMATE_SLACK_BYTES = 16;
    private static final long LOG10_2_LOWER_NUMERATOR = 3_010_299;
    private static final long LOG10_2_UPPER_NUMERATOR = 3_010_300;
    private static final long LOG10_2_DENOMINATOR = 10_000_000;

    private JsonValues() {
    }

    /**
     * Copies an object-shaped JSON value recursively.
     *
     * @param value source object; keys must be non-blank strings
     * @param fieldName field used in validation messages
     * @return immutable object whose nested arrays and objects are also immutable
     */
    public static Map<String, Object> copyObject(Map<String, ?> value, String fieldName) {
        Objects.requireNonNull(value, fieldName + " must not be null");
        Map<String, Object> copied = new LinkedHashMap<>();
        for (Map.Entry<String, ?> entry : value.entrySet()) {
            String key = requireText(entry.getKey(), fieldName + " key");
            copied.put(key, copyValue(entry.getValue(), fieldName + "." + key));
        }
        return Collections.unmodifiableMap(copied);
    }

    /**
     * Copies a JSON value recursively, rejecting host-specific mutable/executable values and non-finite
     * floating-point numbers which have no JSON representation.
     *
     * @param value source JSON value
     * @param fieldName field used in validation messages
     * @return immutable JSON-compatible value
     */
    public static Object copyValue(Object value, String fieldName) {
        if (value instanceof Double doubleValue) {
            if (!Double.isFinite(doubleValue)) {
                throw new IllegalArgumentException(fieldName + " must not contain non-finite Double value");
            }
            return doubleValue;
        }
        if (value instanceof Float floatValue) {
            if (!Float.isFinite(floatValue)) {
                throw new IllegalArgumentException(fieldName + " must not contain non-finite Float value");
            }
            return floatValue;
        }
        if (value == null || value instanceof String || value instanceof Boolean || value instanceof Integer
                || value instanceof Long || value instanceof Short || value instanceof Byte) {
            return value;
        }
        if (value.getClass() == BigDecimal.class || value.getClass() == BigInteger.class) {
            return value;
        }
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> copied = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (!(entry.getKey() instanceof String key)) {
                    throw new IllegalArgumentException(fieldName + " object keys must be strings");
                }
                copied.put(requireText(key, fieldName + " key"), copyValue(entry.getValue(), fieldName + "." + key));
            }
            return Collections.unmodifiableMap(copied);
        }
        if (value instanceof List<?> values) {
            List<Object> copied = new ArrayList<>(values.size());
            for (int index = 0; index < values.size(); index++) {
                copied.add(copyValue(values.get(index), fieldName + "[" + index + "]"));
            }
            return Collections.unmodifiableList(copied);
        }
        throw new IllegalArgumentException(fieldName + " must contain JSON-compatible values");
    }

    /**
     * Measures several independent JSON roots before copying or serializing their contents.
     *
     * <p>The admission footprint uses JSON encoding rules but deliberately counts every map entry,
     * including null-valued entries which Fastjson2 may omit by default. This is a memory-safety
     * boundary, not a promise about a serializer's null omission policy. Limits are checked
     * incrementally with saturated arithmetic before callers build copied field paths.</p>
     *
     * @param roots independent JSON-compatible values whose admission footprints are added together
     * @param limits finite byte, token, depth and node limits
     * @return conservative admission bytes and visited value nodes
     * @throws JsonResourceLimitException when any configured limit is exceeded
     * @throws IllegalArgumentException when a root is not part of the supported JSON value subset
     */
    public static JsonResourceUsage measureJsonResources(
            Iterable<?> roots, JsonResourceLimits limits) {
        Objects.requireNonNull(roots, "JSON resource roots must not be null");
        ResourceCounter counter = new ResourceCounter(
                Objects.requireNonNull(limits, "JSON resource limits must not be null"));
        for (Object root : roots) {
            counter.visit(root, 0);
        }
        return new JsonResourceUsage(counter.encodedBytes, counter.nodes);
    }

    /**
     * Returns the exact Fastjson2 default UTF-8 byte count for one encoded JSON string token.
     *
     * @param value source Java string
     * @return bytes including opening/closing quotes and all escaping
     */
    public static long jsonEncodedStringBytes(String value) {
        Objects.requireNonNull(value, "JSON string must not be null");
        return saturatedAdd(jsonStringContentBytes(value), 2);
    }

    /**
     * Returns JSON string content bytes without the surrounding quotes.
     *
     * @param value source Java string
     * @return escaped UTF-8 bytes inside the quotes
     */
    public static long jsonStringContentBytes(String value) {
        Objects.requireNonNull(value, "JSON string must not be null");
        return jsonStringContentBytes(List.of(value));
    }

    /**
     * Returns the encoded content bytes after concatenating several character sequences.
     *
     * <p>Surrogate halves at adjacent part boundaries are treated exactly as they would be in the
     * resulting Java string, without first allocating that result.</p>
     *
     * @param parts character sequences in concatenation order
     * @return escaped UTF-8 bytes inside the final JSON quotes
     */
    public static long jsonStringContentBytes(Iterable<? extends CharSequence> parts) {
        return measureJsonString(parts).contentBytes();
    }

    /**
     * Measures the final JSON content bytes and Unicode code points of concatenated text parts.
     *
     * @param parts character sequences in concatenation order
     * @return exact content bytes and code points without constructing the concatenated string
     */
    public static JsonStringUsage measureJsonString(Iterable<? extends CharSequence> parts) {
        Objects.requireNonNull(parts, "JSON string parts must not be null");
        long bytes = 0;
        long codePoints = 0;
        boolean pendingHighSurrogate = false;
        for (CharSequence part : parts) {
            Objects.requireNonNull(part, "JSON string part must not be null");
            for (int offset = 0; offset < part.length(); offset++) {
                char unit = part.charAt(offset);
                if (pendingHighSurrogate) {
                    if (Character.isLowSurrogate(unit)) {
                        bytes = saturatedAdd(bytes, 4);
                        codePoints = saturatedAdd(codePoints, 1);
                        pendingHighSurrogate = false;
                        continue;
                    }
                    // Fastjson2 replaces an unmatched surrogate with '?'.
                    bytes = saturatedAdd(bytes, 1);
                    codePoints = saturatedAdd(codePoints, 1);
                    pendingHighSurrogate = false;
                }
                if (Character.isHighSurrogate(unit)) {
                    pendingHighSurrogate = true;
                } else if (Character.isLowSurrogate(unit)) {
                    bytes = saturatedAdd(bytes, 1);
                } else if (unit == '"' || unit == '\\'
                        || unit == '\b' || unit == '\f' || unit == '\n'
                        || unit == '\r' || unit == '\t') {
                    bytes = saturatedAdd(bytes, 2);
                } else if (unit < 0x20) {
                    bytes = saturatedAdd(bytes, 6);
                } else {
                    bytes = saturatedAdd(bytes, unit <= 0x7f ? 1 : unit <= 0x7ff ? 2 : 3);
                }
                if (!Character.isHighSurrogate(unit)) {
                    codePoints = saturatedAdd(codePoints, 1);
                }
            }
        }
        if (pendingHighSurrogate) {
            bytes = saturatedAdd(bytes, 1);
            codePoints = saturatedAdd(codePoints, 1);
        }
        return new JsonStringUsage(bytes, codePoints);
    }

    /**
     * Returns encoded content bytes for a repeated string without constructing the repeated value.
     *
     * @param value source sequence
     * @param repetitions non-negative repetition count
     * @return escaped UTF-8 bytes inside the final JSON quotes
     */
    public static long jsonRepeatedStringContentBytes(String value, long repetitions) {
        Objects.requireNonNull(value, "JSON string must not be null");
        if (repetitions < 0) {
            throw new IllegalArgumentException("JSON string repetitions must not be negative");
        }
        if (repetitions == 0 || value.isEmpty()) {
            return 0;
        }
        long bytes = saturatedMultiply(jsonStringContentBytes(value), repetitions);
        if (Character.isLowSurrogate(value.charAt(0))
                && Character.isHighSurrogate(value.charAt(value.length() - 1))) {
            // Each internal boundary turns two '?' replacement bytes into one 4-byte code point.
            bytes = saturatedAdd(bytes, saturatedMultiply(repetitions - 1, 2));
        }
        return bytes;
    }

    /**
     * Returns the exact default JSON text byte count for a supported finite number.
     *
     * @param value JSON number
     * @return ASCII bytes in its serialized representation
     */
    public static long jsonEncodedNumberBytes(Number value) {
        return jsonEncodedNumberBytes(value, MAX_EXACT_NUMBER_WORKSPACE_BYTES);
    }

    /**
     * Returns exact Fastjson2 number bytes only after its numeric workspace fits a finite bound.
     *
     * @param value supported JSON number
     * @param maxWorkspaceBytes maximum logical digits/text workspace admitted before exact work
     * @return exact ASCII bytes in the Fastjson2 representation
     * @throws JsonResourceLimitException when exact calculation is not proven bounded
     */
    public static long jsonEncodedNumberBytes(Number value, long maxWorkspaceBytes) {
        validateJsonNumber(value);
        requireNumberWorkspace(maxWorkspaceBytes);
        long boundedWorkspace = Math.min(MAX_EXACT_NUMBER_WORKSPACE_BYTES,
                saturatedAdd(maxWorkspaceBytes, NUMBER_ESTIMATE_SLACK_BYTES));
        if (jsonNumberWorkspaceUpperBoundBytes(value) > boundedWorkspace) {
            throw new JsonResourceLimitException();
        }
        long encodedBytes;
        if (value instanceof BigDecimal decimal) {
            encodedBytes = fastjsonBigDecimalBytes(decimal);
        } else {
            encodedBytes = value.toString().length();
        }
        if (encodedBytes > maxWorkspaceBytes) {
            throw new JsonResourceLimitException();
        }
        return encodedBytes;
    }

    /**
     * Returns the exact {@link String#valueOf(Object)} text length for a supported JSON number.
     *
     * <p>BigDecimal exponent/plain selection is calculated arithmetically so a large plain value is
     * not materialized merely to decide whether a later string operation fits its budget.</p>
     *
     * @param value JSON number used as text
     * @return ASCII bytes/code points in Java's canonical number text
     */
    public static long javaNumberTextBytes(Number value) {
        validateJsonNumber(value);
        if (javaNumberTextUpperBoundBytes(value) > MAX_EXACT_NUMBER_WORKSPACE_BYTES) {
            throw new JsonResourceLimitException();
        }
        if (value instanceof BigDecimal decimal) {
            return javaBigDecimalTextBytes(decimal);
        }
        return value.toString().length();
    }

    /**
     * Upper-bounds Java's canonical number text without materializing a large integer string.
     *
     * <p>The BigInteger estimate uses a rational upper bound for {@code log10(2)}. Callers can
     * therefore reserve allocation budget before invoking {@code toString()}.</p>
     *
     * @param value supported JSON number
     * @return ASCII bytes/code points sufficient for its canonical Java text
     */
    public static long javaNumberTextUpperBoundBytes(Number value) {
        validateJsonNumber(value);
        if (value instanceof BigInteger integer) {
            return signedDigitsUpperBound(integer);
        }
        if (value instanceof BigDecimal decimal) {
            return javaBigDecimalWorkspaceUpperBoundBytes(decimal);
        }
        return primitiveNumberTextUpperBoundBytes(value);
    }

    /**
     * Upper-bounds Fastjson2 number encoding and any precision workspace without calling
     * {@link BigDecimal#precision()} or rendering decimal text.
     *
     * @param value supported JSON number
     * @return saturated logical workspace bytes
     */
    public static long jsonNumberWorkspaceUpperBoundBytes(Number value) {
        validateJsonNumber(value);
        if (value instanceof BigDecimal decimal) {
            DigitBounds digits = decimalDigitBounds(decimal.unscaledValue());
            long javaWorkspace = javaBigDecimalWorkspaceUpperBoundBytes(decimal, digits);
            long scaleWorkspace = saturatedAdd(
                    saturatedAdd(digits.upper(), Math.abs((long) decimal.scale())), 3);
            return Math.max(javaWorkspace, scaleWorkspace);
        }
        return javaNumberTextUpperBoundBytes(value);
    }

    /**
     * Upper-bounds numeric arithmetic workspace from magnitude and scale without exact precision.
     *
     * @param value supported JSON number
     * @return saturated logical workspace bytes for an allocating numeric operation
     */
    public static long numericOperationWorkspaceUpperBoundBytes(Number value) {
        validateJsonNumber(value);
        if (value instanceof BigDecimal decimal) {
            return saturatedAdd(
                    saturatedAdd(decimalDigitBounds(decimal.unscaledValue()).upper(),
                            Math.abs((long) decimal.scale())),
                    3);
        }
        return javaNumberTextUpperBoundBytes(value);
    }

    /**
     * Returns whether shared numeric comparison/formatting may proceed under the absolute gate.
     *
     * @param value supported JSON number
     * @return true only when magnitude and scale prove subsequent bounded work safe
     */
    public static boolean isNumberWorkspaceSafelyBounded(Number value) {
        return jsonNumberWorkspaceUpperBoundBytes(value) <= MAX_EXACT_NUMBER_WORKSPACE_BYTES;
    }

    private static void validateJsonNumber(Number value) {
        Objects.requireNonNull(value, "JSON number must not be null");
        if (value instanceof Double doubleValue && !Double.isFinite(doubleValue)
                || value instanceof Float floatValue && !Float.isFinite(floatValue)) {
            throw new IllegalArgumentException("JSON number must be finite");
        }
        if (!(value instanceof Double) && !(value instanceof Float)
                && !(value instanceof Integer) && !(value instanceof Long)
                && !(value instanceof Short) && !(value instanceof Byte)
                && value.getClass() != BigDecimal.class && value.getClass() != BigInteger.class) {
            throw new IllegalArgumentException("number type is outside the JSON value subset");
        }
    }

    private static void requireNumberWorkspace(long maxWorkspaceBytes) {
        if (maxWorkspaceBytes < 1) {
            throw new IllegalArgumentException("number workspace limit must be positive");
        }
    }

    private static long primitiveNumberTextUpperBoundBytes(Number value) {
        // Primitive wrapper text is intrinsically bounded (at most a few dozen bytes), so exact
        // sizing cannot become an attacker-controlled large allocation.
        return value.toString().length();
    }

    private static long signedDigitsUpperBound(BigInteger value) {
        DigitBounds digits = decimalDigitBounds(value);
        return saturatedAdd(digits.upper(), value.signum() < 0 ? 1 : 0);
    }

    private static DigitBounds decimalDigitBounds(BigInteger value) {
        if (value.signum() == 0) {
            return new DigitBounds(1, 1);
        }
        long bitLength = value.bitLength();
        long magnitudeUpperBits = saturatedAdd(bitLength, value.signum() < 0 ? 1 : 0);
        long upper = saturatedAdd(
                saturatedMultiply(magnitudeUpperBits, LOG10_2_UPPER_NUMERATOR)
                        / LOG10_2_DENOMINATOR,
                1);
        long lowerBits = Math.max(0, bitLength - 1);
        long lower = saturatedAdd(
                saturatedMultiply(lowerBits, LOG10_2_LOWER_NUMERATOR)
                        / LOG10_2_DENOMINATOR,
                1);
        return new DigitBounds(Math.min(lower, upper), upper);
    }

    private static long javaBigDecimalWorkspaceUpperBoundBytes(BigDecimal value) {
        return javaBigDecimalWorkspaceUpperBoundBytes(value, decimalDigitBounds(value.unscaledValue()));
    }

    private static long javaBigDecimalWorkspaceUpperBoundBytes(
            BigDecimal value, DigitBounds digits) {
        long textUpper = Math.max(
                javaBigDecimalTextBytes(value.signum(), value.scale(), digits.lower()),
                javaBigDecimalTextBytes(value.signum(), value.scale(), digits.upper()));
        long transitionPrecision = (long) value.scale() - 5;
        if (transitionPrecision >= digits.lower() && transitionPrecision <= digits.upper()) {
            textUpper = Math.max(textUpper,
                    javaBigDecimalTextBytes(value.signum(), value.scale(), transitionPrecision));
            if (transitionPrecision > digits.lower()) {
                textUpper = Math.max(textUpper,
                        javaBigDecimalTextBytes(
                                value.signum(), value.scale(), transitionPrecision - 1));
            }
        }
        return Math.max(textUpper, digits.upper());
    }

    private static long javaBigDecimalTextBytes(int signum, int scale, long precision) {
        long sign = signum < 0 ? 1 : 0;
        long scaleOffset = -((long) scale) - 1;
        long adjustedExponent = scaleOffset >= 0
                ? saturatedAdd(precision, scaleOffset)
                : precision + scaleOffset;
        if (scale >= 0 && adjustedExponent >= -6) {
            return saturatedAdd(sign, plainDecimalBytes(precision, scale));
        }
        long mantissa = saturatedAdd(precision, precision > 1 ? 1 : 0);
        long exponentDigits = decimalDigits(Math.abs(adjustedExponent));
        return saturatedAdd(sign, saturatedAdd(mantissa, saturatedAdd(2, exponentDigits)));
    }

    /** Computes Fastjson2's default BigDecimal text length without materializing plain decimal text. */
    private static long fastjsonBigDecimalBytes(BigDecimal value) {
        long precision = value.precision();
        long sign = value.signum() < 0 ? 1 : 0;
        int scale = value.scale();
        // JSONWriterUTF8 writes compact (<19 digit), non-negative-scale decimals through
        // IOUtils.writeDecimal, whose representation is always plain even where BigDecimal.toString
        // would switch to an exponent.
        if (precision < 19 && scale >= 0) {
            return saturatedAdd(sign, plainDecimalBytes(precision, scale));
        }
        return javaBigDecimalTextBytes(value);
    }

    private static long javaBigDecimalTextBytes(BigDecimal value) {
        long precision = value.precision();
        return javaBigDecimalTextBytes(value.signum(), value.scale(), precision);
    }

    private static long plainDecimalBytes(long precision, long scale) {
        if (scale == 0) {
            return precision;
        }
        return scale < precision ? saturatedAdd(precision, 1) : saturatedAdd(scale, 2);
    }

    private static int decimalDigits(long value) {
        int digits = 1;
        while (value >= 10) {
            value /= 10;
            digits++;
        }
        return digits;
    }

    /** Finite limits used by the allocation-free JSON resource walker. */
    public record JsonResourceLimits(long maxEncodedBytes, long maxEncodedStringBytes,
                                     int maxDepth, int maxNodes) {

        public JsonResourceLimits {
            if (maxEncodedBytes < 1 || maxEncodedStringBytes < 1
                    || maxDepth < 0 || maxNodes < 1) {
                throw new IllegalArgumentException("JSON resource limits must be positive");
            }
        }
    }

    /** Conservative admission resource usage for all measured roots. */
    public record JsonResourceUsage(long encodedBytes, int nodes) {
    }

    /** Exact JSON string content resources across one or more concatenated source parts. */
    public record JsonStringUsage(long contentBytes, long codePoints) {
    }

    private record DigitBounds(long lower, long upper) {
    }

    /** Stable limit failure which never embeds a JSON key or value. */
    public static final class JsonResourceLimitException extends IllegalArgumentException {

        private JsonResourceLimitException() {
            super("JSON value exceeds the configured resource limits");
        }
    }

    private static final class ResourceCounter {

        private final JsonResourceLimits limits;
        private long encodedBytes;
        private int nodes;

        private ResourceCounter(JsonResourceLimits limits) {
            this.limits = limits;
        }

        private void visit(Object value, int depth) {
            if (depth > limits.maxDepth() || ++nodes > limits.maxNodes()) {
                rejectLimit();
            }
            if (value == null) {
                addBytes(4);
                return;
            }
            if (value instanceof String text) {
                addString(text);
                return;
            }
            if (value instanceof Boolean bool) {
                addBytes(bool ? 4 : 5);
                return;
            }
            if (value instanceof Number number) {
                addBytes(jsonEncodedNumberBytes(number, limits.maxEncodedBytes()));
                return;
            }
            if (value instanceof Map<?, ?> map) {
                addBytes(2);
                boolean first = true;
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    if (!(entry.getKey() instanceof String key)) {
                        throw new IllegalArgumentException("JSON object keys must be strings");
                    }
                    if (!first) {
                        addBytes(1);
                    }
                    first = false;
                    addString(key);
                    addBytes(1);
                    visit(entry.getValue(), depth + 1);
                }
                return;
            }
            if (value instanceof List<?> list) {
                addBytes(2);
                for (int index = 0; index < list.size(); index++) {
                    if (index > 0) {
                        addBytes(1);
                    }
                    visit(list.get(index), depth + 1);
                }
                return;
            }
            throw new IllegalArgumentException("value must contain JSON-compatible values");
        }

        private void addString(String value) {
            long bytes = jsonEncodedStringBytes(value);
            if (bytes > limits.maxEncodedStringBytes()) {
                rejectLimit();
            }
            addBytes(bytes);
        }

        private void addBytes(long amount) {
            encodedBytes = saturatedAdd(encodedBytes, amount);
            if (encodedBytes > limits.maxEncodedBytes()) {
                rejectLimit();
            }
        }

        private static void rejectLimit() {
            throw new JsonResourceLimitException();
        }
    }

    private static long saturatedAdd(long left, long right) {
        if (right > 0 && left > Long.MAX_VALUE - right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    private static long saturatedMultiply(long left, long right) {
        if (left != 0 && right > Long.MAX_VALUE / left) {
            return Long.MAX_VALUE;
        }
        return left * right;
    }

    /** Validates and normalizes a required textual identifier. */
    public static String requireText(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName + " must not be null");
        String normalized = value.strip();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return normalized;
    }
}
