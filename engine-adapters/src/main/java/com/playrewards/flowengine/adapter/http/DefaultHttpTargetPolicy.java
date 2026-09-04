package com.playrewards.flowengine.adapter.http;

import com.playrewards.flowengine.contract.execution.ExecutionPurpose;
import com.playrewards.flowengine.spi.HttpTargetPolicy;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Public-Internet-only HTTP target policy with all-answer DNS validation.
 *
 * <p>Every resolution answer must be globally routable. The transport invokes this policy both
 * before sending and after receiving headers, so a changed mixed/private answer fails closed.</p>
 */
public final class DefaultHttpTargetPolicy implements HttpTargetPolicy {

    private final AddressResolver resolver;

    /** Creates the production policy using the host JVM resolver. */
    public DefaultHttpTargetPolicy() {
        this(host -> List.of(InetAddress.getAllByName(host)));
    }

    /** Package-local deterministic DNS seam used by rebinding and mixed-answer tests. */
    DefaultHttpTargetPolicy(AddressResolver resolver) {
        this.resolver = Objects.requireNonNull(resolver, "resolver must not be null");
    }

    /** Validates URI syntax, resolves every address and rejects the whole target if any answer is unsafe. */
    @Override
    public void validate(URI uri, ExecutionPurpose purpose) {
        Objects.requireNonNull(uri, "HTTP target URI must not be null");
        Objects.requireNonNull(purpose, "execution purpose must not be null");
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
        List<InetAddress> addresses;
        try {
            addresses = List.copyOf(resolver.resolve(host));
        } catch (UnknownHostException | RuntimeException failure) {
            throw rejected(failure);
        }
        if (addresses.isEmpty() || addresses.stream().anyMatch(address -> !isPublic(address))) {
            throw rejected(null);
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
        if ((first & 0xfe) == 0xfc || first == 0xff
                || first == 0xfe && (second & 0xc0) == 0x80
                || first == 0x20 && second == 0x01
                && (bytes[2] & 0xff) == 0x0d && (bytes[3] & 0xff) == 0xb8) {
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
}
