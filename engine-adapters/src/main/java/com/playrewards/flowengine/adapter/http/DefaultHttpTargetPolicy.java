package com.playrewards.flowengine.adapter.http;

import com.playrewards.flowengine.contract.execution.ExecutionPurpose;
import com.playrewards.flowengine.spi.CancellationToken;
import com.playrewards.flowengine.spi.HttpTargetPolicy;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Public-Internet-only HTTP target policy with all-answer DNS validation.
 *
 * <p>Every resolution answer must be globally routable. One approval returns immutable concrete
 * addresses which the transport pins for the lifetime of that request; redirects are disabled.</p>
 */
public final class DefaultHttpTargetPolicy implements HttpTargetPolicy {

    private static final int MAX_RESOLUTIONS = 64;
    private static final long DNS_POLL_NANOS = TimeUnit.MILLISECONDS.toNanos(10);
    private static final Semaphore DNS_ADMISSION = new Semaphore(MAX_RESOLUTIONS, true);
    private static final ExecutorService DNS_TASKS = Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("flow-http-dns-", 0).factory());

    private final AddressResolver resolver;
    private final java.util.function.Predicate<InetAddress> publicAddress;

    /** Creates the production policy using the host JVM resolver. */
    public DefaultHttpTargetPolicy() {
        this(host -> List.of(InetAddress.getAllByName(host)), DefaultHttpTargetPolicy::isPublic);
    }

    /** Package-local deterministic DNS seam used by rebinding and mixed-answer tests. */
    DefaultHttpTargetPolicy(AddressResolver resolver) {
        this(resolver, DefaultHttpTargetPolicy::isPublic);
    }

    /** Package-local seam for proving address pinning with isolated loopback fixtures. */
    DefaultHttpTargetPolicy(AddressResolver resolver,
            java.util.function.Predicate<InetAddress> publicAddress) {
        this.resolver = Objects.requireNonNull(resolver, "resolver must not be null");
        this.publicAddress = Objects.requireNonNull(publicAddress, "publicAddress must not be null");
    }

    /** Validates URI syntax, resolves every address and rejects the whole target if any answer is unsafe. */
    @Override
    public ApprovedTarget approve(URI uri, ExecutionPurpose purpose,
            CancellationToken cancellation, Instant deadline) {
        Objects.requireNonNull(uri, "HTTP target URI must not be null");
        Objects.requireNonNull(purpose, "execution purpose must not be null");
        Objects.requireNonNull(cancellation, "cancellation must not be null");
        Objects.requireNonNull(deadline, "deadline must not be null");
        String scheme = uri.getScheme();
        if (scheme == null || !(scheme.equalsIgnoreCase("http") || scheme.equalsIgnoreCase("https"))
                || uri.getRawUserInfo() != null || uri.getRawFragment() != null
                || uri.getPort() == 0 || uri.getPort() < -1 || uri.getPort() > 65_535) {
            throw rejected(null);
        }
        String host = uri.getHost();
        if (host == null || host.isBlank()) {
            throw rejected(null);
        }
        host = unbracket(host);
        if (host.endsWith(".") || host.indexOf('%') >= 0 || !isCanonicalHost(host)) {
            throw rejected(null);
        }
        List<InetAddress> addresses = resolve(host, cancellation, deadline);
        if (addresses.isEmpty() || addresses.stream().anyMatch(address -> !publicAddress.test(address))) {
            throw rejected(null);
        }
        int port = uri.getPort() > 0 ? uri.getPort() : scheme.equalsIgnoreCase("https") ? 443 : 80;
        return new ApprovedTarget(uri, addresses, port);
    }

    private List<InetAddress> resolve(String host, CancellationToken cancellation, Instant deadline) {
        acquireDnsAdmission(cancellation, deadline);
        ResolutionOperation operation = new ResolutionOperation();
        try {
            DNS_TASKS.submit(() -> {
                operation.started(Thread.currentThread());
                List<InetAddress> resolved = null;
                Throwable resolverFailure = null;
                try {
                    resolved = List.copyOf(resolver.resolve(host));
                } catch (Throwable failure) {
                    resolverFailure = failure;
                } finally {
                    operation.finished();
                    DNS_ADMISSION.release();
                }
                if (resolverFailure == null) {
                    operation.completion.complete(resolved);
                } else {
                    operation.completion.completeExceptionally(resolverFailure);
                }
            });
        } catch (RuntimeException failure) {
            DNS_ADMISSION.release();
            throw new TargetRejectedException(TargetFailureKind.CAPACITY,
                    "HTTP target approval capacity unavailable", failure);
        }
        try (CancellationToken.CancellationRegistration ignored =
                cancellation.onCancellation(operation::interrupt)) {
            while (true) {
                cancellation.throwIfCancellationRequested();
                long remaining = remainingNanos(deadline);
                if (remaining <= 0) {
                    operation.interrupt();
                    throw new TargetRejectedException(TargetFailureKind.TIMEOUT,
                            "HTTP target approval timed out", null);
                }
                try {
                    return operation.completion.get(Math.min(DNS_POLL_NANOS, remaining),
                            TimeUnit.NANOSECONDS);
                } catch (TimeoutException ignoredTimeout) {
                    // The exact remaining deadline, not the poll interval, decides expiry.
                } catch (InterruptedException failure) {
                    operation.interrupt();
                    Thread.currentThread().interrupt();
                    throw new CancellationException("HTTP target approval was cancelled");
                } catch (ExecutionException failure) {
                    throw rejected(failure.getCause());
                }
            }
        }
    }

    private static void acquireDnsAdmission(CancellationToken cancellation, Instant deadline) {
        cancellation.throwIfCancellationRequested();
        if (remainingNanos(deadline) <= 0) {
            throw new TargetRejectedException(TargetFailureKind.TIMEOUT,
                    "HTTP target approval timed out", null);
        }
        if (!DNS_ADMISSION.tryAcquire()) {
            throw new TargetRejectedException(TargetFailureKind.CAPACITY,
                    "HTTP target approval capacity unavailable", null);
        }
    }

    private static long remainingNanos(Instant deadline) {
        try {
            return Math.max(0, Duration.between(Instant.now(), deadline).toNanos());
        } catch (ArithmeticException failure) {
            return deadline.isAfter(Instant.now()) ? Long.MAX_VALUE : 0;
        }
    }

    private static boolean isCanonicalHost(String host) {
        if (host.indexOf(':') >= 0) {
            try {
                return InetAddress.getByName(host) instanceof Inet6Address;
            } catch (UnknownHostException failure) {
                return false;
            }
        }
        if (!host.equals(host.toLowerCase(Locale.ROOT))) {
            return false;
        }
        for (int index = 0; index < host.length(); index++) {
            char value = host.charAt(index);
            if (!(value >= 'a' && value <= 'z' || value >= '0' && value <= '9'
                    || value == '-' || value == '.')) {
                return false;
            }
        }
        return !host.startsWith(".") && !host.contains("..") && !host.endsWith("-")
                && Arrays.stream(host.split("\\.")).noneMatch(label -> label.isEmpty()
                        || label.startsWith("-") || label.endsWith("-"));
    }

    private static boolean isPublic(InetAddress address) {
        Objects.requireNonNull(address, "resolved address must not be null");
        if (address.isAnyLocalAddress() || address.isLoopbackAddress() || address.isLinkLocalAddress()
                || address.isSiteLocalAddress() || address.isMulticastAddress()) {
            return false;
        }
        byte[] bytes = address.getAddress();
        if (address instanceof Inet4Address || bytes.length == 4) {
            return publicIpv4(bytes);
        }
        if (!(address instanceof Inet6Address) || bytes.length != 16) {
            return false;
        }
        int first = bytes[0] & 0xff;
        int second = bytes[1] & 0xff;
        // The conservative 2001::/23 denial also contains benchmark 2001:2::/48 and both
        // ORCHID prefixes (2001:10::/28 and 2001:20::/28); do not punch holes into special-use space.
        if ((first & 0xe0) != 0x20
                || first == 0x20 && second == 0x01 && (bytes[2] & 0xfe) == 0
                || first == 0x20 && second == 0x01
                && (bytes[2] & 0xff) == 0x0d && (bytes[3] & 0xff) == 0xb8
                || first == 0x20 && second == 0x02
                || first == 0x3f && second == 0xff && (bytes[2] & 0xf0) == 0) {
            return false;
        }
        if (isIpv4MappedOrCompatible(bytes)) {
            return false;
        }
        return true;
    }

    private static boolean publicIpv4(byte[] bytes) {
        int first = bytes[0] & 0xff;
        int second = bytes[1] & 0xff;
        int third = bytes[2] & 0xff;
        return first != 0 && first != 10 && first != 127 && first < 224
                && !(first == 100 && second >= 64 && second <= 127)
                && !(first == 169 && second == 254)
                && !(first == 172 && second >= 16 && second <= 31)
                && !(first == 192 && second == 168)
                && !(first == 192 && second == 0 && (third == 0 || third == 2))
                && !(first == 192 && second == 88 && third == 99)
                && !(first == 198 && (second == 18 || second == 19))
                && !(first == 198 && second == 51 && third == 100)
                && !(first == 203 && second == 0 && third == 113);
    }

    private static boolean isIpv4MappedOrCompatible(byte[] bytes) {
        for (int index = 0; index < 10; index++) {
            if (bytes[index] != 0) {
                return false;
            }
        }
        return bytes[10] == 0 && bytes[11] == 0 || bytes[10] == (byte) 0xff && bytes[11] == (byte) 0xff;
    }

    private static String unbracket(String host) {
        return host.startsWith("[") && host.endsWith("]") ? host.substring(1, host.length() - 1) : host;
    }

    private static TargetRejectedException rejected(Throwable cause) {
        return cause == null
                ? new TargetRejectedException("HTTP target rejected")
                : new TargetRejectedException("HTTP target rejected", cause);
    }

    /** Resolver abstraction which returns every answer for one canonical host. */
    @FunctionalInterface
    interface AddressResolver {
        List<InetAddress> resolve(String host) throws UnknownHostException;
    }

    private static final class ResolutionOperation {

        private final CompletableFuture<List<InetAddress>> completion = new CompletableFuture<>();
        private final AtomicReference<Thread> worker = new AtomicReference<>();
        private final AtomicBoolean stopRequested = new AtomicBoolean();

        private void started(Thread running) {
            worker.set(running);
            if (stopRequested.get()) {
                running.interrupt();
            }
        }

        private void interrupt() {
            stopRequested.set(true);
            Thread running = worker.get();
            if (running != null) {
                running.interrupt();
            }
        }

        private void finished() {
            worker.set(null);
        }
    }
}
