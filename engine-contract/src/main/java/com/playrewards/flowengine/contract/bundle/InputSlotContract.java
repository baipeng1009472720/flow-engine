package com.playrewards.flowengine.contract.bundle;

import com.playrewards.flowengine.contract.json.JsonValues;
import com.playrewards.flowengine.contract.protocol.ProtocolSchema;
import java.util.Objects;

/**
 * Immutable input-slot declaration preserved in an executable bundle.
 *
 * <p>The map key is the stable machine key; {@code name} is the display name and {@code required}
 * controls compile-time binding completeness.</p>
 */
public record InputSlotContract(String name, boolean required, ProtocolSchema protocol) {

    public InputSlotContract {
        name = JsonValues.requireText(name, "input slot name");
        Objects.requireNonNull(protocol, "input slot protocol must not be null");
    }
}
