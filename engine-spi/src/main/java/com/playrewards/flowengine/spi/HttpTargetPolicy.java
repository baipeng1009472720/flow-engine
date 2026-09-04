package com.playrewards.flowengine.spi;

import com.playrewards.flowengine.contract.execution.ExecutionPurpose;
import java.net.InetAddress;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Resolves and approves one HTTP destination before transport connection.
 *
 * <p>Implementations own DNS and address classification. Core supplies only the immutable URI and
 * execution purpose. The returned address set is the transport's immutable connection authority;
 * transports must never resolve the URI host again.</p>
 */
@FunctionalInterface
public interface HttpTargetPolicy {

    /**
     * Resolves every address and returns the exact finite set a transport may connect to.
     *
     * @param uri requested destination
     * @param purpose production or isolated trial policy selector
     * @param cancellation cooperative DNS/admission cancellation
     * @param deadline absolute upper bound for target approval
     * @return immutable URI/address/port approval
     */
    ApprovedTarget approve(URI uri, ExecutionPurpose purpose,
            CancellationToken cancellation, Instant deadline);

    /** Compatibility validation entry point for configuration/tests which do not start transport. */
    default void validate(URI uri, ExecutionPurpose purpose) {
        approve(uri, purpose, () -> false, Instant.MAX);
    }

    /** Immutable connection authority; every address was checked by the selected purpose policy. */
    record ApprovedTarget(URI uri, List<InetAddress> addresses, int port) {

        public ApprovedTarget {
            Objects.requireNonNull(uri, "approved URI must not be null");
            Objects.requireNonNull(addresses, "approved addresses must not be null");
            addresses = List.copyOf(addresses);
            if (addresses.isEmpty() || addresses.stream().anyMatch(Objects::isNull)) {
                throw new IllegalArgumentException("approved addresses must not be empty");
            }
            if (port < 1 || port > 65_535) {
                throw new IllegalArgumentException("approved port is invalid");
            }
        }
    }

    /** Stable pre-send policy outcome used by the transport failure mapper. */
    enum TargetFailureKind {
        REJECTED,
        TIMEOUT,
        CAPACITY
    }

    /** Safe policy rejection which never includes the rejected raw URI. */
    final class TargetRejectedException extends RuntimeException {

        private final TargetFailureKind kind;

        /** Creates a redaction-safe target rejection. */
        public TargetRejectedException(String ignoredMessage) {
            this(TargetFailureKind.REJECTED, ignoredMessage, null);
        }

        /** Creates a redaction-safe target rejection while discarding the potentially sensitive cause. */
        public TargetRejectedException(String ignoredMessage, Throwable cause) {
            this(TargetFailureKind.REJECTED, ignoredMessage, cause);
        }

        /** Creates a redaction-safe classified approval failure. */
        public TargetRejectedException(TargetFailureKind kind, String ignoredMessage, Throwable cause) {
            // DNS/URI causes can contain target material, so the public policy exception discards it.
            super(safeMessage(kind));
            this.kind = Objects.requireNonNull(kind, "target failure kind must not be null");
        }

        /** @return stable target approval failure classification */
        public TargetFailureKind kind() {
            return kind;
        }

        private static String safeMessage(TargetFailureKind kind) {
            Objects.requireNonNull(kind, "target failure kind must not be null");
            return switch (kind) {
                case REJECTED -> "HTTP target rejected by policy";
                case TIMEOUT -> "HTTP target approval timed out";
                case CAPACITY -> "HTTP target approval capacity unavailable";
            };
        }
    }
}
