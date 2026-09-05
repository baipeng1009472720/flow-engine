package com.playrewards.flowengine.spi;

/** Stores are resolved together for one authenticated tenant, never from a process thread-local. */
public record DurableStores(
        FencedExecutionStateStore states, ExecutionLeaseRepository leases, PayloadStore payloads) {}
