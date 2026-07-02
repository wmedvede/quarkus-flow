package io.quarkiverse.flow.opentelemetry;

import static io.quarkiverse.flow.opentelemetry.OtelWorkflowExecutionListener.printThreadAndCurrentVertxContext;

import jakarta.inject.Inject;
import jakarta.ws.rs.client.Invocation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapSetter;
import io.serverlessworkflow.impl.TaskContext;
import io.serverlessworkflow.impl.WorkflowContext;
import io.serverlessworkflow.impl.executors.http.HttpRequestDecorator;

public class CDIOtelHttpRequestDecorator implements HttpRequestDecorator {
    private static final Logger LOGGER = LoggerFactory.getLogger(CDIOtelHttpRequestDecorator.class);
    @Inject
    InstrumentationContextManager contextManager;

    @Override
    public void decorate(Invocation.Builder decorated, WorkflowContext workflowContext, TaskContext taskContext) {
        String workflowInstanceId = workflowContext.instanceData().id();
        String taskId = taskContext.position().jsonPointer();
        int iteration = taskContext.iteration();
        short retryAttempt = taskContext.retryAttempt();

        LOGGER.debug("Decorating request for workflowInstanceId: " + workflowInstanceId + ", taskId: " + taskId
                + ", iteration: " + iteration + ", retryAttempt: " + retryAttempt);

        InstrumentationContext taskInstanceContext = contextManager.getTaskInstanceContext(workflowInstanceId, taskId,
                iteration, retryAttempt);

        if (taskInstanceContext == null) {
            LOGGER.warn("No taskInstanceContext was found for workflowInstanceId: " + workflowInstanceId
                    + ", taskId: " + taskId + ", iteration: " + iteration + ", retryAttempt: " + retryAttempt);
            return;
        }

        TextMapSetter<Invocation.Builder> setter = (carrier, key, value) -> {
            if (carrier != null) {
                LOGGER.debug(" setting request header key: " + key + " with value: " + value);
                carrier.header(key, value);
            }
        };

        printThreadAndCurrentVertxContext("DECORATING - taskId: " + taskId);

        Context propagtedContext = taskInstanceContext.getStartSpan().storeInContext(taskInstanceContext.getParentContext());
        GlobalOpenTelemetry.getPropagators().getTextMapPropagator().inject(
                propagtedContext,
                decorated,
                setter);
    }
}
