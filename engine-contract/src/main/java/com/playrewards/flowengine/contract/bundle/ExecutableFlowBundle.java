package com.playrewards.flowengine.contract.bundle;

import com.playrewards.flowengine.contract.flow.FlowDefinition;
import com.playrewards.flowengine.contract.json.ContractJson;
import com.playrewards.flowengine.contract.protocol.ProtocolSchema;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Complete immutable static input for one flow execution.
 *
 * <p>The bundle combines a FlowDefinition with all resolved component and protocol contracts so an
 * execution never rereads host metadata that might drift. The hash excludes itself and trust signature
 * fields to avoid self-reference and signing cycles.</p>
 */
public record ExecutableFlowBundle(
        String contractVersion,
        FlowIdentity flowIdentity,
        FlowDefinition flowDefinition,
        Map<String, CompiledComponentContract> componentContracts,
        Map<String, ProtocolSchema> protocolSchemas,
        Map<String, ProtocolSchema> externalScopeSchemas,
        String bundleHash,
        BundleTrust trust) {

    public static final String CURRENT_CONTRACT_VERSION = "1.0";

    public ExecutableFlowBundle {
        Objects.requireNonNull(contractVersion, "contractVersion must not be null");
        if (!CURRENT_CONTRACT_VERSION.equals(contractVersion.strip())) {
            throw new IllegalArgumentException("unsupported contractVersion: " + contractVersion);
        }
        contractVersion = CURRENT_CONTRACT_VERSION;
        Objects.requireNonNull(flowIdentity, "flowIdentity must not be null");
        Objects.requireNonNull(flowDefinition, "flowDefinition must not be null");
        componentContracts = copyComponents(componentContracts);
        protocolSchemas = copySchemas(protocolSchemas, "protocolSchemas");
        externalScopeSchemas = copySchemas(externalScopeSchemas, "externalScopeSchemas");
        if (bundleHash != null && !bundleHash.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("bundleHash must be a lowercase SHA-256 hex digest");
        }
    }

    /**
     * Computes the SHA-256 digest of this bundle's canonical JSON representation.
     *
     * @return lowercase hexadecimal digest excluding {@code bundleHash} and {@code trust.signature}
     */
    public String calculateBundleHash() {
        return ContractJson.bundleHash(this);
    }

    /**
     * Returns a copy carrying the canonical hash. The original value remains untouched.
     *
     * @return bundle with a verified-format calculated hash
     */
    public ExecutableFlowBundle withCalculatedHash() {
        return new ExecutableFlowBundle(contractVersion, flowIdentity, flowDefinition, componentContracts,
                protocolSchemas, externalScopeSchemas, calculateBundleHash(), trust);
    }

    private static Map<String, CompiledComponentContract> copyComponents(
            Map<String, CompiledComponentContract> contracts) {
        Objects.requireNonNull(contracts, "componentContracts must not be null");
        Map<String, CompiledComponentContract> copied = new LinkedHashMap<>();
        for (Map.Entry<String, CompiledComponentContract> entry : contracts.entrySet()) {
            copied.put(requireKey(entry.getKey(), "componentContracts"),
                    Objects.requireNonNull(entry.getValue(), "componentContracts values must not be null"));
        }
        return Map.copyOf(copied);
    }

    private static Map<String, ProtocolSchema> copySchemas(Map<String, ProtocolSchema> schemas,
                                                            String fieldName) {
        Objects.requireNonNull(schemas, fieldName + " must not be null");
        Map<String, ProtocolSchema> copied = new LinkedHashMap<>();
        for (Map.Entry<String, ProtocolSchema> entry : schemas.entrySet()) {
            copied.put(requireKey(entry.getKey(), fieldName),
                    Objects.requireNonNull(entry.getValue(), fieldName + " values must not be null"));
        }
        return Map.copyOf(copied);
    }

    private static String requireKey(String key, String fieldName) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException(fieldName + " keys must not be blank");
        }
        return key.strip();
    }
}
