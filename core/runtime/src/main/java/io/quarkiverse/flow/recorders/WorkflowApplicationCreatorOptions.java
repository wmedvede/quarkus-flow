package io.quarkiverse.flow.recorders;

public class WorkflowApplicationCreatorOptions {

    private boolean micrometerSupported;

    private boolean otelSupported;

    public WorkflowApplicationCreatorOptions() {
        this(false, false);
    }

    public WorkflowApplicationCreatorOptions(boolean micrometerSupported, boolean otelSupported) {
        this.micrometerSupported = micrometerSupported;
        this.otelSupported = otelSupported;
    }

    public boolean isMicrometerSupported() {
        return micrometerSupported;
    }

    public boolean isOtelSupported() {
        return otelSupported;
    }
}
