package com.playrewards.flowengine.server;

import com.playrewards.flowengine.core.fixture.ProhibitedSpringDependency;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Verifies that the engine's contract, core, and SPI remain independent from framework and
 * legacy PlayRewards implementation packages. This test does not validate runtime behavior.
 */
@AnalyzeClasses(packages = "com.playrewards.flowengine", importOptions = ImportOption.DoNotIncludeTests.class)
final class ArchitectureTest {

    @ArchTest
    static final ArchRule core_is_framework_independent = frameworkIndependenceRule();

    /**
     * Ensures the rule itself rejects a deliberately isolated test fixture, so an empty target
     * package cannot accidentally make the architecture gate appear meaningful.
     */
    @Test
    void framework_independence_rule_rejects_spring_dependency_in_protected_package() {
        assertThrows(AssertionError.class, () -> frameworkIndependenceRule()
                .check(new ClassFileImporter().importClasses(ProhibitedSpringDependency.class)));
    }

    private static ArchRule frameworkIndependenceRule() {
        return noClasses()
                .that().resideInAnyPackage("..contract..", "..core..", "..spi..")
                .should().dependOnClassesThat()
                .resideInAnyPackage("org.springframework..", "com.playrewards.domain..",
                        "com.playrewards.application..", "com.baomidou.mybatisplus..");
    }
}
