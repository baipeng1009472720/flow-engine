package com.playrewards.flowengine.server;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.playrewards.flowengine.adapter.fixture.ProhibitedAdapterToCoreDependency;
import com.playrewards.flowengine.contract.fixture.ProhibitedContractToCoreDependency;
import com.playrewards.flowengine.core.fixture.ProhibitedCoreToAdapterDependency;
import com.playrewards.flowengine.core.fixture.ProhibitedJavaSqlDependency;
import com.playrewards.flowengine.core.fixture.ProhibitedJdkHttpServerDependency;
import com.playrewards.flowengine.core.fixture.ProhibitedMySqlDependency;
import com.playrewards.flowengine.core.fixture.ProhibitedPlayRewardsDomainDependency;
import com.playrewards.flowengine.core.fixture.ProhibitedSpringDependency;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.Test;

/**
 * Verifies that the engine's contract, core, and SPI remain independent from framework and
 * legacy PlayRewards implementation packages. This test does not validate runtime behavior.
 */
@AnalyzeClasses(packages = "com.playrewards.flowengine", importOptions = ImportOption.DoNotIncludeTests.class)
final class ArchitectureTest {

    @ArchTest
    static final ArchRule core_is_framework_independent = frameworkIndependenceRule();

    @ArchTest
    static final ArchRule contract_and_spi_do_not_depend_on_implementation_modules = contractAndSpiRule();

    @ArchTest
    static final ArchRule core_does_not_depend_on_adapters_or_server = coreLayerRule();

    @ArchTest
    static final ArchRule adapters_do_not_depend_on_core_or_server = adaptersLayerRule();

    /**
     * Ensures the rule itself rejects a deliberately isolated test fixture, so an empty target
     * package cannot accidentally make the architecture gate appear meaningful.
     */
    @Test
    void framework_independence_rule_rejects_spring_dependency_in_protected_package() {
        assertThrows(AssertionError.class, () -> frameworkIndependenceRule()
                .check(new ClassFileImporter().importClasses(ProhibitedSpringDependency.class)));
    }

    @Test
    void framework_independence_rule_rejects_java_sql_dependency_in_protected_package() {
        assertThrows(AssertionError.class, () -> frameworkIndependenceRule()
                .check(new ClassFileImporter().importClasses(ProhibitedJavaSqlDependency.class)));
    }

    @Test
    void framework_independence_rule_rejects_jdk_http_server_dependency_in_protected_package() {
        assertThrows(AssertionError.class, () -> frameworkIndependenceRule()
                .check(new ClassFileImporter().importClasses(ProhibitedJdkHttpServerDependency.class)));
    }

    @Test
    void framework_independence_rule_rejects_mysql_driver_dependency_in_protected_package() {
        assertThrows(AssertionError.class, () -> frameworkIndependenceRule()
                .check(new ClassFileImporter().importClasses(ProhibitedMySqlDependency.class)));
    }

    @Test
    void framework_independence_rule_rejects_playrewards_domain_dependency_in_protected_package() {
        assertThrows(AssertionError.class, () -> frameworkIndependenceRule()
                .check(new ClassFileImporter().importClasses(ProhibitedPlayRewardsDomainDependency.class)));
    }

    @Test
    void module_layer_rules_reject_forbidden_dependencies() {
        ClassFileImporter importer = new ClassFileImporter();
        assertThrows(AssertionError.class, () -> contractAndSpiRule()
                .check(importer.importClasses(ProhibitedContractToCoreDependency.class)));
        assertThrows(AssertionError.class, () -> coreLayerRule()
                .check(importer.importClasses(ProhibitedCoreToAdapterDependency.class)));
        assertThrows(AssertionError.class, () -> adaptersLayerRule()
                .check(importer.importClasses(ProhibitedAdapterToCoreDependency.class)));
    }

    private static ArchRule frameworkIndependenceRule() {
        return noClasses()
                .that().resideInAnyPackage("..contract..", "..core..", "..spi..")
                .should().dependOnClassesThat()
                .resideInAnyPackage("org.springframework..", "com.playrewards.domain..",
                        "com.playrewards.application..", "com.baomidou.mybatisplus..", "java.sql..",
                        "javax.sql..", "com.mysql..", "com.sun.net.httpserver..");
    }

    private static ArchRule contractAndSpiRule() {
        return noClasses()
                .that().resideInAnyPackage("..contract..", "..spi..")
                .should().dependOnClassesThat()
                .resideInAnyPackage("..core..", "..adapter..", "..server..");
    }

    private static ArchRule coreLayerRule() {
        return noClasses()
                .that().resideInAnyPackage("..core..")
                .should().dependOnClassesThat()
                .resideInAnyPackage("..adapter..", "..server..");
    }

    private static ArchRule adaptersLayerRule() {
        // 约束本工程模块，不把已批准的 mybatis-plus.core / flyway.core 当作引擎 core。
        return noClasses()
                .that().resideInAnyPackage("..adapter..")
                .should().dependOnClassesThat()
                .resideInAnyPackage("com.playrewards.flowengine.core..", "com.playrewards.flowengine.server..");
    }
}
