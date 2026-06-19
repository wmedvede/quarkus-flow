package io.quarkiverse.flow.opentelemetry;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InstrumentationContextManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(InstrumentationContextManager.class);
    private final Map<String, InstrumentationContext> workflowInstanceContext = new ConcurrentHashMap<>();
    private final Map<String, Map<String, InstrumentationContext>> workflowInstanceTaskContext = new ConcurrentHashMap<>();

    public InstrumentationContext getWorkflowInstanceContext(String workflowInstanceId) {
        return workflowInstanceContext.get(workflowInstanceId);
    }

    public void removeWorkflowInstanceContext(String workflowInstanceId) {
        workflowInstanceContext.remove(workflowInstanceId);
        workflowInstanceTaskContext.remove(workflowInstanceId);
    }

    public void putWorkflowInstanceContext(String workflowInstanceId, InstrumentationContext context) {
        workflowInstanceContext.put(workflowInstanceId, context);
        workflowInstanceTaskContext.putIfAbsent(workflowInstanceId, new HashMap<>());
    }

    public static String taskContextId(String taskInstanceId, int iteration, short retryAttempt) {
        return taskInstanceId + "-" + iteration + "-" + retryAttempt;
    }

    public void putTaskInstanceInstanceContext(String workflowInstanceId, String taskInstanceId, int iteration,
            short retryAttempt, InstrumentationContext context) {
        workflowInstanceTaskContext.putIfAbsent(workflowInstanceId, new HashMap<>())
                .put(taskContextId(taskInstanceId, iteration, retryAttempt), context);
    }

    public void removeTaskInstanceInstanceContext(String workflowInstanceId, String taskInstanceId, int iteration,
            short retryAttempt) {
        workflowInstanceTaskContext.get(workflowInstanceId).remove(taskContextId(taskInstanceId, iteration, retryAttempt));
    }

    public InstrumentationContext getTaskInstanceContext(String workflowInstanceId, String taskInstanceId, int iteration,
            short retryAttempt) {
        return workflowInstanceTaskContext.get(workflowInstanceId).get(taskContextId(taskInstanceId, iteration, retryAttempt));
    }

    public InstrumentationContext findParentContext(String workflowInstanceId, String jsonPosition) {
        Map<String, InstrumentationContext> currentWorkflowInstanceTaskSpanContext = workflowInstanceTaskContext
                .get(workflowInstanceId);
        InstrumentationContext parentInstrumentationContext = null;
        for (InstrumentationContext instrumentationContext : currentWorkflowInstanceTaskSpanContext.values()) {
            if (jsonPosition.startsWith(instrumentationContext.getJsonPosition()) && (parentInstrumentationContext == null
                    || instrumentationContext.getJsonPosition().length() > parentInstrumentationContext.getJsonPosition()
                            .length())) {
                parentInstrumentationContext = instrumentationContext;
            }
        }
        if (parentInstrumentationContext != null) {
            return parentInstrumentationContext;
        }
        return workflowInstanceContext.get(workflowInstanceId);
    }

    public String findParentContextId(String workflowInstanceId, String jsonPosition) {
        Map<String, InstrumentationContext> currentWorkflowInstanceTaskSpanContext = workflowInstanceTaskContext
                .get(workflowInstanceId);
        InstrumentationContext parentInstrumentationContext = null;
        for (InstrumentationContext instrumentationContext : currentWorkflowInstanceTaskSpanContext.values()) {
            if (jsonPosition.startsWith(instrumentationContext.getJsonPosition())
                    && !jsonPosition.equals(instrumentationContext.getJsonPosition()) && (parentInstrumentationContext == null
                            || instrumentationContext.getJsonPosition().length() > parentInstrumentationContext
                                    .getJsonPosition().length())) {
                parentInstrumentationContext = instrumentationContext;
            }
        }
        if (parentInstrumentationContext != null) {
            return parentInstrumentationContext.getJsonPosition();
        }
        return null;
    }

    public InstrumentationContext findEnclosingParentContext(String workflowInstanceId, String jsonPosition) {
        String parentContextId = findParentContextId(workflowInstanceId, jsonPosition);
        if (parentContextId == null) {
            return workflowInstanceContext.get(workflowInstanceId);
        }
        Map<String, InstrumentationContext> currentWorkflowInstanceTaskSpanContext = workflowInstanceTaskContext
                .get(workflowInstanceId);
        InstrumentationContext parentInstrumentationContext = null;
        for (InstrumentationContext instrumentationContext : currentWorkflowInstanceTaskSpanContext.values()) {
            if (parentContextId.equals(instrumentationContext.getJsonPosition())
                    && (parentInstrumentationContext == null
                            || instrumentationContext.getIteration() > parentInstrumentationContext.getIteration())) {
                parentInstrumentationContext = instrumentationContext;
            }
        }
        return parentInstrumentationContext;
    }

    public void ensureAllTaskSpansAreClosed(String workflowInstanceId) {
        Map<String, InstrumentationContext> tasksContext = workflowInstanceTaskContext.get(workflowInstanceId);
        if (tasksContext == null) {
            LOGGER.warn("No tasks instrumentation context map was found for workflowInstanceId: " + workflowInstanceId);
            return;
        }
        tasksContext.entrySet().stream()
                .sorted(Comparator
                        .comparing((Map.Entry<String, InstrumentationContext> entry) -> entry.getValue().getStartTime())
                        .reversed())
                .forEach(entry -> {
                    if (entry.getValue().getStartSpan() != null) {
                        entry.getValue().getStartSpan().end();
                    }
                });
    }
}
