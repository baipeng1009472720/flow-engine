package com.playrewards.flowengine.core.protocol;

import com.playrewards.flowengine.contract.bundle.OutputSlotContract;
import com.playrewards.flowengine.core.compile.CompiledNode;
import com.playrewards.flowengine.core.node.JsonValueValidator;
import com.playrewards.flowengine.core.node.NodeExecutionException;
import com.playrewards.flowengine.core.node.NodeResult;
import com.playrewards.flowengine.spi.ComponentOutputs;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Validates returned named values without inventing requiredness for declared output slots. */
public final class OutputValidator {

    /**
     * Validates a business-component completion and maps it to the standard NEXT result.
     *
     * @param node compiled output declaration
     * @param outputs returned named component outputs
     * @return immutable validated node result
     */
    public NodeResult validate(CompiledNode node, ComponentOutputs outputs) {
        Objects.requireNonNull(outputs, "component outputs must not be null");
        return validate(node, outputs.values());
    }

    /**
     * Validates returned named JSON values and maps them to the standard NEXT result.
     *
     * <p>Unknown values fail closed. Declared-but-absent outputs stay absent because the contract
     * intentionally has no output required flag.</p>
     *
     * @param node compiled output declaration
     * @param outputs returned named JSON values
     * @return immutable validated node result
     */
    public NodeResult validate(CompiledNode node, Map<String, Object> outputs) {
        validateValues(node, outputs);
        return NodeResult.continueWith(outputs, Set.of("NEXT"));
    }

    /**
     * Validates an existing scheduler result without changing its control ports or terminal outcome.
     *
     * @param node compiled output declaration
     * @param result executor-produced result
     */
    public void validateResult(CompiledNode node, NodeResult result) {
        Objects.requireNonNull(result, "node result must not be null");
        validateValues(node, result.outputs());
    }

    private static void validateValues(CompiledNode node, Map<String, Object> outputs) {
        Objects.requireNonNull(node, "compiled node must not be null");
        Objects.requireNonNull(outputs, "node outputs must not be null");
        for (Map.Entry<String, Object> output : outputs.entrySet()) {
            OutputSlotContract slot = node.componentContract().outputSlots().get(output.getKey());
            if (slot == null) {
                throw new NodeExecutionException("NODE_OUTPUT_UNKNOWN",
                        "node produced undeclared output: " + output.getKey());
            }
            JsonValueValidator.validate(output.getValue(), slot.protocol(),
                    "NODE_OUTPUT_INVALID", "outputs." + output.getKey());
        }
    }
}
