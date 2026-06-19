package io.quarkiverse.flow.opentelemetry;

import java.util.NoSuchElementException;

import io.serverlessworkflow.impl.lifecycle.WorkflowCancelledEvent;
import io.serverlessworkflow.impl.lifecycle.WorkflowCompletedEvent;
import io.serverlessworkflow.impl.lifecycle.WorkflowEvent;
import io.serverlessworkflow.impl.lifecycle.WorkflowFailedEvent;
import io.serverlessworkflow.impl.lifecycle.WorkflowResumedEvent;
import io.serverlessworkflow.impl.lifecycle.WorkflowStartedEvent;
import io.serverlessworkflow.impl.lifecycle.WorkflowSuspendedEvent;

public enum WorkflowEventType {
    WORKFLOW_STARTED,
    WORKFLOW_SUSPENDED,
    WORKFLOW_RESUMED,
    WORKFLOW_COMPLETED,
    WORKFLOW_CANCELLED,
    WORKFLOW_FAILED;

    static WorkflowEventType fromEvent(WorkflowEvent event) {
        if (event instanceof WorkflowStartedEvent) {
            return WORKFLOW_STARTED;
        }
        if (event instanceof WorkflowSuspendedEvent) {
            return WORKFLOW_SUSPENDED;
        }
        if (event instanceof WorkflowResumedEvent) {
            return WORKFLOW_RESUMED;
        }
        if (event instanceof WorkflowCompletedEvent) {
            return WORKFLOW_COMPLETED;
        }
        if (event instanceof WorkflowCancelledEvent) {
            return WORKFLOW_CANCELLED;
        }
        if (event instanceof WorkflowFailedEvent) {
            return WORKFLOW_FAILED;
        }
        throw new NoSuchElementException("WorkflowEvent: " + event.getClass() + " is not recognized.");
    }
}
