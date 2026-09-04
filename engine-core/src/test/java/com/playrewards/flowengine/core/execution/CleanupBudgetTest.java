package com.playrewards.flowengine.core.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Verifies structured cleanup uses one monotonic bounded budget independent of business clocks. */
class CleanupBudgetTest {

    @Test
    void one_monotonic_budget_is_consumed_across_repeated_interrupts() throws Exception {
        CleanupBudget budget = CleanupBudget.start(Duration.ofMillis(100));
        CountDownLatch never = new CountDownLatch(1);
        long started = System.nanoTime();
        Thread worker = Thread.startVirtualThread(() -> {
            while (budget.remainingNanos() > 0) {
                try {
                    never.await(budget.remainingNanos(), TimeUnit.NANOSECONDS);
                } catch (InterruptedException ignored) {
                    // A cleanup retry must consume the original budget rather than reset it.
                }
            }
        });

        for (int attempt = 0; attempt < 5; attempt++) {
            worker.interrupt();
            Thread.sleep(5);
        }
        worker.join(1_000);

        assertTrue(!worker.isAlive());
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        assertTrue(elapsedMillis >= 50, "interrupts must not bypass the cleanup wait");
        assertTrue(elapsedMillis < 1_000, "cleanup must remain bounded by monotonic time");
        assertEquals(0, budget.remainingNanos());
    }
}
