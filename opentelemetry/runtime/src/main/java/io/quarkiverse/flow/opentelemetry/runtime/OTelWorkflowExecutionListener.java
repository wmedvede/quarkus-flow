package io.quarkiverse.flow.opentelemetry.runtime;

import static io.quarkiverse.flow.opentelemetry.runtime.SpanUtils.appendTaskEvent;
import static io.quarkiverse.flow.opentelemetry.runtime.SpanUtils.appendWorkflowEvent;
import static io.quarkiverse.flow.opentelemetry.runtime.SpanUtils.generateTaskSpanName;
import static io.quarkiverse.flow.opentelemetry.runtime.SpanUtils.generateWorkflowSpanName;
import static io.quarkiverse.flow.opentelemetry.runtime.WorkflowInstrumentationContext.getWorkflowInstrumentationContext;
import static io.quarkiverse.flow.opentelemetry.runtime.WorkflowInstrumentationContext.setWorkflowInstrumentationContext;
import static io.serverlessworkflow.impl.lifecycle.EventType.TASK_CANCELLED;
import static io.serverlessworkflow.impl.lifecycle.EventType.TASK_COMPLETED;
import static io.serverlessworkflow.impl.lifecycle.EventType.TASK_RESUMED;
import static io.serverlessworkflow.impl.lifecycle.EventType.TASK_SUSPENDED;
import static io.serverlessworkflow.impl.lifecycle.EventType.WORKFLOW_CANCELLED;
import static io.serverlessworkflow.impl.lifecycle.EventType.WORKFLOW_COMPLETED;
import static io.serverlessworkflow.impl.lifecycle.EventType.WORKFLOW_RESUMED;
import static io.serverlessworkflow.impl.lifecycle.EventType.WORKFLOW_SUSPENDED;

import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentelemetry.api.trace.*;
import io.opentelemetry.context.Context;
import io.quarkiverse.flow.opentelemetry.runtime.config.FlowOTelConfig;
import io.quarkus.runtime.ShutdownEvent;
import io.serverlessworkflow.api.types.TaskBase;
import io.serverlessworkflow.impl.WorkflowPosition;
import io.serverlessworkflow.impl.lifecycle.*;
import io.smallrye.config.Config;

public class OTelWorkflowExecutionListener implements WorkflowExecutionListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(OTelWorkflowExecutionListener.class);

    /**
     * Experimental Quarkus Flow development only property, please don't use.
     */
    @ConfigProperty(name = "quarkus.flow.otel.task.name-strategy", defaultValue = "action-and-task-name")
    SpanUtils.TaskNameStrategy taskNameStrategy;

    @Inject
    SpanBuilderFactory spanBuilderFactory;

    @Inject
    FlowOTelConfig oTelConfig;

    ConcurrentHashMap<String, Span> workflowRunSpan = new ConcurrentHashMap<>();

    ConcurrentHashMap<String, WorkflowInstrumentationContext> workflowInstrumentationContextRegistry = new ConcurrentHashMap<>();

    @Override
    public void onWorkflowStarted(WorkflowStartedEvent ev) {
        if (!oTelConfig.isEnabled()) {
            return;
        }
        WorkflowEventInfo eventInfo = WorkflowEventInfo.from(ev);
        logWorkflowEvent(eventInfo);

        Context parentContext = Context.current();
        Span startSpan = spanBuilderFactory.tracer.spanBuilder("workflow.start").setParent(parentContext)
                .setAttribute("flow.workflow.name", eventInfo.wfName()).startSpan();

        // Save the traceId and spanId in the WF data.
        String traceId = startSpan.getSpanContext().getTraceId(); // 32-hex-character string
        String spanId = startSpan.getSpanContext().getSpanId(); // 16-hex-character string

        System.out.println("XXX - StartWorkflow workflowInstanceId: + " + eventInfo.wfInstanceId() +
                ", traceId: " + traceId + ", spanId: " + spanId);

        String workflowSpanName = generateWorkflowSpanName(eventInfo.wfName());

        Span runSpan = spanBuilderFactory.newWorkflowSpan(workflowSpanName, eventInfo,
                startSpan.storeInContext(parentContext)).startSpan();
        startSpan.end();
        workflowRunSpan.put(eventInfo.wfInstanceId(), runSpan);

        appendWorkflowEvent(runSpan, eventInfo.eventType());

        InstrumentationContext workflowInstanceContext = InstrumentationContext.newBuilder()
                .parentContext(parentContext)
                .withStartSpan(runSpan)
                .withStartTime(Instant.now())
                .build();

        WorkflowInstrumentationContext workflowInstrumentationContext = new WorkflowInstrumentationContext(
                workflowInstanceContext);
        setWorkflowInstrumentationContext(ev.workflowContext().instanceData(), workflowInstrumentationContext);
        workflowInstrumentationContextRegistry.put(eventInfo.wfInstanceId(), workflowInstrumentationContext);
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
        if (!oTelConfig.isEnabled()) {
            return;
        }
        WorkflowEventInfo eventInfo = WorkflowEventInfo.from(ev);
        logWorkflowEvent(eventInfo);

        WorkflowInstrumentationContext workflowContext = getWorkflowInstrumentationContext(ev.workflowContext().instanceData());
        if (workflowContext == null) {
            warnNoWorkflowContext(eventInfo);
            return;
        }

        Span startSpan = workflowContext.getWorkflowInstanceContext().getStartSpan();
        if (eventInfo.eventType() == WORKFLOW_SUSPENDED || eventInfo.eventType() == WORKFLOW_RESUMED) {
            appendWorkflowEvent(startSpan, eventInfo.eventType());
        } else if (eventInfo.eventType() == WORKFLOW_COMPLETED || eventInfo.eventType() == WORKFLOW_CANCELLED) {
            startSpan.setStatus(StatusCode.OK);
            workflowContext.ensureAllTaskSpansAreClosed();
            appendWorkflowEvent(startSpan, eventInfo.eventType());
            startSpan.end();
        } else {
            WorkflowFailedEvent failedEvent = (WorkflowFailedEvent) ev;
            startSpan.recordException(failedEvent.cause());
            startSpan.setStatus(StatusCode.ERROR, failedEvent.cause().getMessage());
            workflowContext.ensureAllTaskSpansAreClosed();
            appendWorkflowEvent(startSpan, eventInfo.eventType());
            startSpan.end();
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
        if (!oTelConfig.isEnabled()) {
            return;
        }
        TaskEventInfo eventInfo = TaskEventInfo.from(ev);
        logTaskEvent(eventInfo);

        WorkflowInstrumentationContext workflowContext = getWorkflowInstrumentationContext(ev.workflowContext().instanceData());

        // Ask the persisted model to know if we come from a resumed execution.
        if (workflowContext == null) {

            //            PersistibleOtelContext persistedContext = contextRepository.findById(eventInfo.wfInstanceId());

            String workflowInstanceId = Config.get().getOptionalValue("workflowInstanceId", String.class).orElse(null);
            String traceId = Config.get().getOptionalValue("traceId", String.class).orElse(null);
            String spanId = Config.get().getOptionalValue("spanId", String.class).orElse(null);

            if (workflowInstanceId == null) {
                // no persisted context
                warnNoWorkflowContext(eventInfo);
                return;
            } else {

                System.out.println(
                        "XXX + Creating new workflow run span from existing start traceId: " + traceId + ", spanId: " + spanId);
                Context parentContext = Context.current();
                String workflowSpanName = generateWorkflowSpanName(eventInfo.wfName());

                WorkflowEventInfo wfWorkflowEventInfo = new WorkflowEventInfo(eventInfo.wfApplicationId(),
                        eventInfo.wfNamespace(), eventInfo.wfName(), eventInfo.wfVersion(), eventInfo.wfInstanceId(),
                        EventType.WORKFLOW_STARTED);

                //TODO, link with the original workflow Starting Span if any.
                SpanContext startSpanContext = SpanContext.createFromRemoteParent(
                        traceId,
                        spanId,
                        TraceFlags.getSampled(),
                        TraceState.getDefault());

                // add link here
                Span runSpan = spanBuilderFactory.newWorkflowSpan(workflowSpanName, wfWorkflowEventInfo,
                        parentContext, startSpanContext).startSpan();

                workflowRunSpan.put(eventInfo.wfInstanceId(), runSpan);

                appendWorkflowEvent(runSpan, eventInfo.eventType());

                InstrumentationContext workflowInstanceContext = InstrumentationContext.newBuilder()
                        .parentContext(parentContext)
                        .withStartSpan(runSpan)
                        .withStartTime(Instant.now())
                        .build();

                workflowContext = new WorkflowInstrumentationContext(workflowInstanceContext);
                setWorkflowInstrumentationContext(ev.workflowContext().instanceData(), workflowContext);
                workflowInstrumentationContextRegistry.put(eventInfo.wfInstanceId(), workflowContext);
            }
        }
        InstrumentationContext parentTaskContext = workflowContext.findEnclosingParentContext(eventInfo.taskId());
        Context parentContext = parentTaskContext.getStartSpan().storeInContext(parentTaskContext.getParentContext());
        String spanName = generateTaskSpanName(taskNameStrategy, eventInfo.taskId(), eventInfo.taskName(),
                eventInfo.taskInstanceIteration(), eventInfo.taskInstanceRetryAttempt());

        SpanBuilder builder = spanBuilderFactory.newTaskSpan(spanName, eventInfo, parentContext);
        enrichSpan(builder, ev.taskContext().task());
        Span startSpan = builder.startSpan();

        appendTaskEvent(startSpan, ev.type());

        InstrumentationContext taskInstanceContext = InstrumentationContext.newBuilder()
                .withJsonPosition(eventInfo.taskId())
                .withTaskType(eventInfo.taskType())
                .withContainerPosition(containerContextPosition(eventInfo.taskType(), ev.taskContext().position()))
                .withStartSpan(startSpan)
                .withStartTime(Instant.now())
                .parentContext(parentContext)
                .withIteration(eventInfo.taskInstanceIteration())
                .withRetrying(eventInfo.taskInstanceRetrying())
                .withRetryAttempt(eventInfo.taskInstanceRetryAttempt())
                .build();

        workflowContext.putTaskInstanceInstanceContext(eventInfo.taskId(),
                eventInfo.taskInstanceIteration(),
                eventInfo.taskInstanceRetryAttempt(), taskInstanceContext);
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

    private void doTaskEvent(TaskEvent ev) {
        if (!oTelConfig.isEnabled()) {
            return;
        }
        TaskEventInfo eventInfo = TaskEventInfo.from(ev);
        logTaskEvent(eventInfo);

        WorkflowInstrumentationContext workflowContext = getWorkflowInstrumentationContext(ev.workflowContext().instanceData());
        if (workflowContext == null) {
            warnNoWorkflowContext(eventInfo);
            return;
        }
        InstrumentationContext taskInstanceContext = workflowContext.getTaskInstanceContext(eventInfo.taskId(),
                eventInfo.taskInstanceIteration(), eventInfo.taskInstanceRetryAttempt());
        if (taskInstanceContext == null) {
            LOGGER.warn(
                    "No taskInstanceContext was found for taskType: {}, taskName: {}, taskId: {}, iteration: {}, isRetrying: {}, retryAttempt: {}",
                    eventInfo.taskType(), eventInfo.taskName(), eventInfo.taskId(), eventInfo.taskInstanceIteration(),
                    eventInfo.taskInstanceRetrying(), eventInfo.taskInstanceRetryAttempt());
            return;
        }

        if (TASK_CANCELLED == eventInfo.eventType() || TASK_COMPLETED == eventInfo.eventType()) {
            Span startSpan = taskInstanceContext.getStartSpan();
            appendTaskEvent(startSpan, eventInfo.eventType());
            startSpan.setStatus(StatusCode.OK);
            startSpan.end();
            workflowContext.removeTaskInstanceInstanceContext(eventInfo.taskId(),
                    eventInfo.taskInstanceIteration(),
                    eventInfo.taskInstanceRetryAttempt());
        } else if (TASK_SUSPENDED == eventInfo.eventType() || TASK_RESUMED == eventInfo.eventType()) {
            Span startSpan = taskInstanceContext.getStartSpan();
            appendTaskEvent(startSpan, eventInfo.eventType());
        } else {
            Span startSpan = taskInstanceContext.getStartSpan();
            TaskFailedEvent failedEvent = (TaskFailedEvent) ev;
            appendTaskEvent(startSpan, eventInfo.eventType());
            startSpan.recordException(failedEvent.cause());
            startSpan.setStatus(StatusCode.ERROR, failedEvent.cause().getMessage());
            startSpan.end();
            workflowContext.removeTaskInstanceInstanceContext(eventInfo.taskId(),
                    eventInfo.taskInstanceIteration(),
                    eventInfo.taskInstanceRetryAttempt());
        }
    }

    @Override
    public void close() {
        WorkflowExecutionListener.super.close();
    }

    private void onShutdown(@Observes ShutdownEvent e) {
        // Every workflow executing on this JVM (new instance, or a resumed durable instance after restart
        // has an active workflow run span and several
        // We must also
        //        if (!e.isStandardShutdown()) {
        System.out.println("XXX - System is going down: isStandardShutdown: " + e.isStandardShutdown());
        // ensure we call end() on all currently open workflow.run tasks.

        workflowRunSpan.forEach((workflowInstanceId, runSpan) -> {
            LOGGER.debug("Ending workflow run span {} - {} before shutdown.", workflowInstanceId, runSpan);
            WorkflowInstrumentationContext instrumentationContext = workflowInstrumentationContextRegistry
                    .get(workflowInstanceId);
            instrumentationContext.failActiveTaskSpans("Task execution interrupted due to JVM shutdown",
                    SpanUtils.ERROR_TYPE_RUNTIME_JVM_SHUTDOWN);
            runSpan.setStatus(StatusCode.ERROR, "Workflow execution interrupted due to JVM shutdown");
            runSpan.setAttribute("error.type", SpanUtils.ERROR_TYPE_RUNTIME_JVM_SHUTDOWN);
            runSpan.end();
        });
        //        }
    }

    private static String containerContextPosition(TaskType taskType, WorkflowPosition position) {
        switch (taskType) {
            case DO:
            case FOR:
            case LISTEN:
            case TRY:
            case FORK:
                int lastSize = position.last().toString().length();
                String jsonPointer = position.jsonPointer();
                return jsonPointer.substring(0, jsonPointer.length() - lastSize - 1);
            default:
                return null;
        }
    }

    private static void logWorkflowEvent(WorkflowEventInfo eventInfo) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    "On - {}:, workflowApplicationId: {}, workflowNamespace: {}, workflowName: {}, workflowInstanceId: {}, workflowVersion: {}",
                    eventInfo.eventType(), eventInfo.wfApplicationId(), eventInfo.wfNamespace(), eventInfo.wfName(),
                    eventInfo.wfInstanceId(), eventInfo.wfVersion());
        }
    }

    private static void logTaskEvent(TaskEventInfo eventInfo) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    "On - {}: taskName: {}, taskType: {}, taskId: {}, iteration: {}, isRetrying: {}, retryAttempt: {}, retryCount: {}",
                    eventInfo.eventType(), eventInfo.taskName(), eventInfo.taskType(), eventInfo.taskId(),
                    eventInfo.taskInstanceIteration(), eventInfo.taskInstanceRetrying(), eventInfo.taskInstanceRetryAttempt(),
                    eventInfo.taskInstanceRetryCount());
        }
    }

    private static void warnNoWorkflowContext(WorkflowEventInfo eventInfo) {
        LOGGER.warn(
                "No instrumentation context was found for workflowApplicationId: {}, workflowNamespace: {}, workflowName: {}, workflowInstanceId: {}, workflowVersion: {}",
                eventInfo.wfApplicationId(), eventInfo.wfNamespace(), eventInfo.wfName(),
                eventInfo.wfInstanceId(), eventInfo.wfVersion());
    }

    private static void warnNoWorkflowContext(TaskEventInfo eventInfo) {
        LOGGER.warn(
                "No instrumentation context was found for workflowApplicationId: {}, workflowNamespace: {}, workflowName: {}, workflowInstanceId: {}, workflowVersion: {} and, taskType: {}, taskName: {}, taskId: {}, iteration: {}, isRetrying: {}, retryAttempt: {}",
                eventInfo.wfApplicationId(), eventInfo.wfNamespace(), eventInfo.wfName(),
                eventInfo.wfInstanceId(), eventInfo.wfVersion(), eventInfo.taskType(), eventInfo.taskName(), eventInfo.taskId(),
                eventInfo.taskInstanceIteration(), eventInfo.taskInstanceRetrying(), eventInfo.taskInstanceRetryCount());
    }

    private void enrichSpan(SpanBuilder span, TaskBase task) {
        SpanUtils.getTaskSpanEnricher(task).enrich(span, task);
    }
}
