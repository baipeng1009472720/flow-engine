package com.playrewards.flowengine.contract.component;

/** Runtime component types recognized by the current persisted FlowDefinition vocabulary. */
public enum ComponentType {
    START, END, IF, MERGE, SWITCH, PARALLEL, LOOP, VARIABLE, SCRIPT, SUB_FLOW, LOCK, HTTP, BUSINESS
}
