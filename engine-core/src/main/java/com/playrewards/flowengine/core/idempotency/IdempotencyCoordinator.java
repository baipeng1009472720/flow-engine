package com.playrewards.flowengine.core.idempotency;

import com.playrewards.flowengine.contract.json.JsonValues;
import com.playrewards.flowengine.core.execution.ScopePath;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/**
 * Derives non-reversible stable node-scope idempotency identities.
 *
 * <p>This coordinator deliberately does not reuse the execution-start {@code IdempotencyStore} as
 * a node-result store. It guarantees stable keys for logical retries; durable committed-result
 * claim/reuse remains an explicit persistence concern.</p>
 */
public final class IdempotencyCoordinator {

    /**
     * Hashes every isolation dimension with length-prefix framing to prevent concatenation collisions.
     */
    public String nodeKey(String tenantId, String requestKey, String flowVersion,
            String nodeCode, ScopePath scopePath) {
        Objects.requireNonNull(scopePath, "scopePath must not be null");
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            add(digest, JsonValues.requireText(tenantId, "idempotency tenantId"));
            add(digest, JsonValues.requireText(requestKey, "request idempotency key"));
            add(digest, JsonValues.requireText(flowVersion, "flow version"));
            add(digest, JsonValues.requireText(nodeCode, "node code"));
            add(digest, scopePath.value());
            return "fe-" + HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    private static void add(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }
}
