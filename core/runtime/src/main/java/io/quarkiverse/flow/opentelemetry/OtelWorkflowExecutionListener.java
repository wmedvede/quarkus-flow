package io.quarkiverse.flow.opentelemetry;

import static io.quarkiverse.flow.opentelemetry.EventUtil.shortEventName;
import static io.quarkiverse.flow.opentelemetry.TaskEventType.TASK_CANCELLED;
import static io.quarkiverse.flow.opentelemetry.TaskEventType.TASK_COMPLETED;
import static io.quarkiverse.flow.opentelemetry.TaskEventType.TASK_RESUMED;
import static io.quarkiverse.flow.opentelemetry.TaskEventType.TASK_SUSPENDED;
import static io.quarkiverse.flow.opentelemetry.WorkflowEventType.WORKFLOW_CANCELLED;
import static io.quarkiverse.flow.opentelemetry.WorkflowEventType.WORKFLOW_COMPLETED;
import static io.quarkiverse.flow.opentelemetry.WorkflowEventType.WORKFLOW_RESUMED;
import static io.quarkiverse.flow.opentelemetry.WorkflowEventType.WORKFLOW_SUSPENDED;

import java.time.Instant;

import jakarta.inject.Inject;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;
import io.serverlessworkflow.impl.TaskContext;
import io.serverlessworkflow.impl.lifecycle.TaskCancelledEvent;
import io.serverlessworkflow.impl.lifecycle.TaskCompletedEvent;
import io.serverlessworkflow.impl.lifecycle.TaskEvent;
import io.serverlessworkflow.impl.lifecycle.TaskFailedEvent;
import io.serverlessworkflow.impl.lifecycle.TaskResumedEvent;
import io.serverlessworkflow.impl.lifecycle.TaskRetriedEvent;
import io.serverlessworkflow.impl.lifecycle.TaskStartedEvent;
import io.serverlessworkflow.impl.lifecycle.TaskSuspendedEvent;
import io.serverlessworkflow.impl.lifecycle.WorkflowCancelledEvent;
import io.serverlessworkflow.impl.lifecycle.WorkflowCompletedEvent;
import io.serverlessworkflow.impl.lifecycle.WorkflowEvent;
import io.serverlessworkflow.impl.lifecycle.WorkflowExecutionListener;
import io.serverlessworkflow.impl.lifecycle.WorkflowFailedEvent;
import io.serverlessworkflow.impl.lifecycle.WorkflowResumedEvent;
import io.serverlessworkflow.impl.lifecycle.WorkflowStartedEvent;
import io.serverlessworkflow.impl.lifecycle.WorkflowSuspendedEvent;

public class OtelWorkflowExecutionListener implements WorkflowExecutionListener {

    public enum SpanNameGenerationMode {
        TASK_TYPE_AND_NAME,
        TASK_ID_AND_NAME
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(OtelWorkflowExecutionListener.class);

    @ConfigProperty(name = "quarkus.flow.span-wfName-generation-mode", defaultValue = "TASK_TYPE_AND_NAME")
    SpanNameGenerationMode spanNameGenerationMode;

    @Inject
    SpanBuilderFactory spanBuilderFactory;

    @Inject
    InstrumentationContextManager contextManager;

    @Override
    public void onWorkflowStarted(WorkflowStartedEvent ev) {
        WorkflowEventInfo eventInfo = WorkflowEventInfo.from(ev);

        LOGGER.debug("On " + eventInfo.eventType() + ": workflowApplicationId: " + eventInfo.wfApplicationId()
                + ", workflowNamespace: "
                + eventInfo.wfNamespace() + ", workflowName: " + eventInfo.wfName() + ", workflowInstanceId: "
                + eventInfo.wfInstanceId()
                + ", workflowVersion: " + eventInfo.wfVersion());

        Context parentContext = Context.current();
        String workflowSpanName = generateWorkflowSpanName(eventInfo.wfName());

        Span startSpan = spanBuilderFactory.newWorkflowSpan(workflowSpanName,
                eventInfo.wfApplicationId(),
                eventInfo.wfNamespace(),
                eventInfo.wfName(),
                eventInfo.wfInstanceId(),
                eventInfo.wfVersion(),
                // The start process span is always a child of the ongoing context.
                // What happens when:
                // 1) We execute the blocking creation
                // 2) We execute the fire and forget creation
                // 3) We execute the scheduled driven creation (every, cron, after, on)
                parentContext).startSpan();
        appendWorkflowEvent(startSpan, ev);

        InstrumentationContext workflowContext = InstrumentationContext.newBuilder()
                .parentContext(parentContext)
                .withStartSpan(startSpan)
                .withStartTime(Instant.now())
                .build();

        contextManager.putWorkflowInstanceContext(eventInfo.wfInstanceId(), workflowContext);
    }

    @Override
    public void onWorkflowSuspended(WorkflowSuspendedEvent ev) {
        doWorkflowEvent(ev);
    }

    @Override
    public void onWorkflowResumed(WorkflowResumedEvent ev) {
        doWorkflowEvent(ev);
    }

    @Override
    public void onWorkflowCompleted(WorkflowCompletedEvent ev) {
        doWorkflowEvent(ev);
    }

    @Override
    public void onWorkflowCancelled(WorkflowCancelledEvent ev) {
        doWorkflowEvent(ev);
    }

    @Override
    public void onWorkflowFailed(WorkflowFailedEvent ev) {
        doWorkflowEvent(ev);
    }

    private void doWorkflowEvent(WorkflowEvent ev) {
        WorkflowEventInfo eventInfo = WorkflowEventInfo.from(ev);

        LOGGER.debug("On" + eventInfo.eventType() + ": workflowApplicationId: " + eventInfo.wfApplicationId()
                + ", workflowNamespace: "
                + eventInfo.wfNamespace() + "workflowName: " + eventInfo.wfName() + ", workflowInstanceId: "
                + eventInfo.wfInstanceId() + ", workflowVersion: "
                + eventInfo.wfVersion());

        InstrumentationContext workflowContext = contextManager.getWorkflowInstanceContext(eventInfo.wfInstanceId());
        if (workflowContext == null) {
            LOGGER.warn("On" + eventInfo.eventType() + ": no instrumentation context was found for  workflowApplicationId: "
                    + eventInfo.wfApplicationId() + ", workflowNamespace: " + eventInfo.wfNamespace()
                    + "workflowName: " + eventInfo.wfName() + ", workflowInstanceId: " + eventInfo.wfInstanceId()
                    + ", workflowVersion: "
                    + eventInfo.wfVersion());
            return;
        }

        Span startSpan = workflowContext.getStartSpan();
        if (eventInfo.eventType() == WORKFLOW_SUSPENDED || eventInfo.eventType() == WORKFLOW_RESUMED) {
            appendWorkflowEvent(workflowContext.getStartSpan(), ev);
        } else if (eventInfo.eventType() == WORKFLOW_COMPLETED || eventInfo.eventType() == WORKFLOW_CANCELLED) {
            startSpan.setStatus(StatusCode.OK);
            contextManager.ensureAllTaskSpansAreClosed(eventInfo.wfInstanceId());
            appendWorkflowEvent(startSpan, ev);
            startSpan.end();
            contextManager.removeWorkflowInstanceContext(eventInfo.wfInstanceId());
        } else {
            WorkflowFailedEvent failedEvent = (WorkflowFailedEvent) ev;
            startSpan.recordException(failedEvent.cause());
            startSpan.setStatus(StatusCode.ERROR, failedEvent.cause().getMessage());
            contextManager.ensureAllTaskSpansAreClosed(eventInfo.wfInstanceId());
            appendWorkflowEvent(startSpan, ev);
            startSpan.end();
            contextManager.removeWorkflowInstanceContext(eventInfo.wfInstanceId());
        }
    }

    @Override
    public void onTaskStarted(TaskStartedEvent ev) {
        doTaskStartedOrRetried(ev);
    }

    @Override
    public void onTaskRetried(TaskRetriedEvent ev) {
        doTaskStartedOrRetried(ev);
    }

    private void doTaskStartedOrRetried(TaskEvent ev) {
        TaskEventInfo eventInfo = TaskEventInfo.from(ev);

        LOGGER.debug("On" + eventInfo.eventType() + " taskType: " + eventInfo.taskType() + ", taskName: " + eventInfo.taskName()
                + ", taskId: " + eventInfo.taskId()
                + ", iteration: " + eventInfo.taskInstanceIteration() + ", isRetrying: " + eventInfo.taskInstanceRetrying()
                + ", retryAttempt: "
                + eventInfo.taskInstanceRetryAttempt() + ", retryCount: " + eventInfo.taskInstanceRetryCount());

        InstrumentationContext parentTaskContext = contextManager.findEnclosingParentContext(eventInfo.wfInstanceId(),
                eventInfo.taskId());
        Context parentContext = parentTaskContext.getStartSpan().storeInContext(parentTaskContext.getParentContext());
        String spanName = generateTaskSpanName(spanNameGenerationMode, eventInfo.taskId(), eventInfo.taskType(),
                eventInfo.taskName(),
                eventInfo.taskInstanceIteration(), eventInfo.taskInstanceRetryAttempt());

        Span startSpan = spanBuilderFactory.newTaskSpan(spanName,
                eventInfo.wfApplicationId(),
                eventInfo.wfNamespace(),
                eventInfo.wfName(),
                eventInfo.wfInstanceId(),
                eventInfo.wfVersion(),
                eventInfo.wfInstanceId(),
                eventInfo.taskType().name(),
                eventInfo.taskName(),
                eventInfo.taskInstanceIteration(),
                eventInfo.taskInstanceRetrying(),
                eventInfo.taskInstanceRetryAttempt(),
                parentContext)
                .startSpan();

        appendTaskEvent(startSpan, ev);

        InstrumentationContext taskInstanceContext = InstrumentationContext.newBuilder()
                .withJsonPosition(eventInfo.taskId())
                .withTaskType(eventInfo.taskType())
                .withStartSpan(startSpan)
                .withStartTime(Instant.now())
                .parentContext(parentContext)
                .withIteration(eventInfo.taskInstanceIteration())
                .withRetrying(eventInfo.taskInstanceRetrying())
                .withRetryAttempt(eventInfo.taskInstanceRetryAttempt())
                .build();

        contextManager.putTaskInstanceInstanceContext(eventInfo.wfInstanceId(), eventInfo.taskId(),
                eventInfo.taskInstanceIteration(),
                eventInfo.taskInstanceRetryAttempt(), taskInstanceContext);

        if (eventInfo.taskType() == TaskType.SET) {
            // To emulate the instrumentation of the work of the potential real work.
            ((TaskContext) ev.taskContext()).variables().put("otel-task-span", startSpan);
        }
    }

    private void doTaskEvent(TaskEvent ev) {
        TaskEventInfo eventInfo = TaskEventInfo.from(ev);

        LOGGER.debug("On" + eventInfo.eventType() + " taskType: " + eventInfo.taskType() + ", taskName: " + eventInfo.taskName()
                + ", taskId: " + eventInfo.taskId()
                + ", iteration: " + eventInfo.taskInstanceIteration() + ", isRetrying: " + eventInfo.taskInstanceRetrying()
                + ", retryAttempt: "
                + eventInfo.taskInstanceRetryAttempt() + ", retryCount: " + eventInfo.taskInstanceRetryCount());

        InstrumentationContext taskInstanceContext = contextManager.getTaskInstanceContext(eventInfo.wfInstanceId(),
                eventInfo.taskId(),
                eventInfo.taskInstanceIteration(), eventInfo.taskInstanceRetryAttempt());
        if (taskInstanceContext == null) {
            LOGGER.warn("No taskInstanceContext was found for taskType: " + eventInfo.taskType() + ", taskName: "
                    + eventInfo.taskName() + " taskId: "
                    + eventInfo.taskId() + " iteration: " + eventInfo.taskInstanceIteration() + " isRetrying: "
                    + eventInfo.taskInstanceRetrying()
                    + " retryAttempt: " + eventInfo.taskInstanceRetryAttempt());
            return;
        }

        if (TASK_CANCELLED == eventInfo.eventType() || TASK_COMPLETED == eventInfo.eventType()) {
            Span startSpan = taskInstanceContext.getStartSpan();
            appendTaskEvent(startSpan, ev);
            startSpan.setStatus(StatusCode.OK);
            taskInstanceContext.getStartSpan().end();
            contextManager.removeTaskInstanceInstanceContext(eventInfo.wfInstanceId(), eventInfo.taskId(),
                    eventInfo.taskInstanceIteration(),
                    eventInfo.taskInstanceRetryAttempt());
        } else if (TASK_SUSPENDED == eventInfo.eventType() || TASK_RESUMED == eventInfo.eventType()) {
            Span startSpan = taskInstanceContext.getStartSpan();
            appendTaskEvent(startSpan, ev);
        } else {
            Span startSpan = taskInstanceContext.getStartSpan();
            TaskFailedEvent failedEvent = (TaskFailedEvent) ev;
            appendTaskEvent(startSpan, ev);
            startSpan.recordException(failedEvent.cause());
            startSpan.setStatus(StatusCode.ERROR, failedEvent.cause().getMessage());
            taskInstanceContext.getStartSpan().end();
            contextManager.removeTaskInstanceInstanceContext(eventInfo.wfInstanceId(), eventInfo.taskId(),
                    eventInfo.taskInstanceIteration(),
                    eventInfo.taskInstanceRetryAttempt());
        }
    }

    @Override
    public void onTaskCompleted(TaskCompletedEvent ev) {
        doTaskEvent(ev);
    }

    @Override
    public void onTaskCancelled(TaskCancelledEvent ev) {
        doTaskEvent(ev);
    }

    @Override
    public void onTaskFailed(TaskFailedEvent ev) {
        doTaskEvent(ev);
    }

    @Override
    public void onTaskSuspended(TaskSuspendedEvent ev) {
        doTaskEvent(ev);
    }

    @Override
    public void onTaskResumed(TaskResumedEvent ev) {
        doTaskEvent(ev);
    }

    private static void appendWorkflowEvent(Span span, WorkflowEvent ev) {
        span.addEvent(shortEventName(ev));
    }

    private static void appendTaskEvent(Span span, TaskEvent ev) {
        span.addEvent(shortEventName(ev));
    }

    private static String generateTaskSpanName(
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

    private static String generateWorkflowSpanName(
            String workflowName) {
        return "WORKFLOW (" + workflowName + ")";
    }

    @Override
    public void close() {
        WorkflowExecutionListener.super.close();
    }

}
