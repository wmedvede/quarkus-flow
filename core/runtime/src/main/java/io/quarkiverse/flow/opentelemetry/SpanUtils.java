package io.quarkiverse.flow.opentelemetry;

import io.opentelemetry.api.trace.Span;

public class SpanUtils {
    private SpanUtils() {
    }

    public enum SpanNameGenerationMode {
        TASK_TYPE_AND_NAME,
        TASK_ID_AND_NAME
    }

    public static void appendWorkflowEvent(Span span, WorkflowEventType eventType) {
        span.addEvent(eventType.id());
    }

    public static void appendTaskEvent(Span span, TaskEventType eventType) {
        span.addEvent(eventType.id());
    }

    public static String generateTaskSpanName(
            SpanNameGenerationMode generationMode,
            String taskInstanceId,
            TaskType taskType,
            String taskName,
            int taskInstanceIteration, short retryAttempt) {
        if (generationMode == SpanNameGenerationMode.TASK_TYPE_AND_NAME) {
            return taskType + ": " + taskName + " iteration: " + taskInstanceIteration + ", retry: " + retryAttempt;
        }
        return taskInstanceId + "-" + " (" + taskName + ") # " + taskInstanceIteration + "(retry: " + retryAttempt + ")";
    }

    public static String generateWorkflowSpanName(
            String workflowName) {
        return "WORKFLOW (" + workflowName + ")";
    }
}
