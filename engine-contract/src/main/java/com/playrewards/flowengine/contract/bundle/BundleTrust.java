package com.playrewards.flowengine.contract.bundle;

/**
 * Provenance information attached to a bundle by a trusted host.
 *
 * <p>The signature is deliberately not included in the bundle hash and never appears in diagnostic
 * text. Verification policy belongs to the server or host adapter, not this value contract.</p>
 */
public record BundleTrust(String issuer, String keyId, String signature) {

    @Override
    public String toString() {
        return "BundleTrust[issuer=" + issuer + ", keyId=" + keyId + ", signature=<redacted>]";
    }
}
