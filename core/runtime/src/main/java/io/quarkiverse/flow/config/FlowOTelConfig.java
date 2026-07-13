package io.quarkiverse.flow.config;

import java.util.Optional;

import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;

@ConfigMapping(prefix = "quarkus.flow.otel")
@ConfigRoot(phase = ConfigPhase.RUN_TIME)
public interface FlowOTelConfig {

    /**
     * Enable OpenTelemetry for Quarkus Flows.
     * <p>
     * In all cases, the support for Quarkus Flows OpenTelemetry is enabled only if the following conditions are met:
     * 1) The quarkus-opentelemetry extension is configured in the given project.
     * 2) The quarkus.otel.enabled is true.
     * 3) The quarkus.otel.traces.enabled us true.
     */
    Optional<Boolean> enabled();

    /**
     * Use this method the access current configured value, or default value.
     * To distinguish default value from user explicitly configured value use enabled() instead.
     *
     * @return returns the currently configured value or the default value when not configured.
     */
    default boolean isEnabled() {
        return enabled().orElse(true);
    }
}
