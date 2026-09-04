package com.playrewards.flowengine.contract.bundle;

import com.playrewards.flowengine.contract.json.JsonValues;
import com.playrewards.flowengine.contract.protocol.ProtocolSchema;
import java.util.Objects;

/**
 * Immutable output-slot declaration preserved in an executable bundle.
 *
 * <p>Outputs deliberately have no required flag: requiredness belongs to the consuming input.</p>
 */
public record OutputSlotContract(String name, ProtocolSchema protocol) {

    public OutputSlotContract {
        name = JsonValues.requireText(name, "output slot name");
        Objects.requireNonNull(protocol, "output slot protocol must not be null");
    }
}
