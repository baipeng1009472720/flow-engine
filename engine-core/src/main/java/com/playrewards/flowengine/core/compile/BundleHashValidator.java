package com.playrewards.flowengine.core.compile;

import com.playrewards.flowengine.contract.bundle.ExecutableFlowBundle;
import java.util.List;

/** Shared trusted-entry validation that prevents cache hits from bypassing bundle integrity. */
final class BundleHashValidator {

    private BundleHashValidator() {
    }

    static List<CompilationIssue> issues(ExecutableFlowBundle bundle) {
        if (bundle.bundleHash() == null || bundle.bundleHash().isBlank()) {
            return List.of(new CompilationIssue(
                    "FLOW_BUNDLE_HASH_REQUIRED", null, "bundleHash", "bundleHash 不能为空"));
        }
        if (!bundle.bundleHash().equals(bundle.calculateBundleHash())) {
            return List.of(new CompilationIssue(
                    "FLOW_BUNDLE_HASH_MISMATCH", null, "bundleHash", "bundleHash 与规范化 bundle 内容不一致"));
        }
        return List.of();
    }

    static void validate(ExecutableFlowBundle bundle) {
        List<CompilationIssue> issues = issues(bundle);
        if (!issues.isEmpty()) {
            throw new FlowCompilationException(issues);
        }
    }
}
