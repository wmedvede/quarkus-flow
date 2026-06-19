package io.quarkiverse.flow.opentelemetry;

import io.serverlessworkflow.impl.lifecycle.TaskEvent;
import io.serverlessworkflow.impl.lifecycle.WorkflowEvent;

public class EventUtil {

    public static String shortEventName(TaskEvent ev) {
        return clipSuffix(ev.getClass().getSimpleName());
    }

    public static String clipSuffix(String name) {
        return name.substring(0, name.length() - "Event".length());
    }

    public static String shortEventName(WorkflowEvent ev) {
        return clipSuffix(ev.getClass().getSimpleName());
    }

}
