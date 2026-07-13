package io.quarkiverse.flow.opentelemetry;

import jakarta.ws.rs.client.Invocation;

import io.quarkus.arc.Arc;
import io.serverlessworkflow.impl.TaskContext;
import io.serverlessworkflow.impl.WorkflowContext;
import io.serverlessworkflow.impl.executors.http.HttpRequestDecorator;

public class OTelHttpRequestDecorator implements HttpRequestDecorator {
    private static volatile CDIOTelHttpRequestDecorator DELEGATE;
    private static final Object LOCK = new Object();

    @Override
    public void decorate(Invocation.Builder decoratedBuilder, WorkflowContext workflowContext, TaskContext taskContext) {
        boolean cancelDecoration = true;
        System.out.println("XXXXXXXXXXXXXXXXXXXXXXXXX DECORATOR 1111");
        if (cancelDecoration) {
            System.out.println("XXXXXXXXXXXXXXXXXXXXXXXXX DECORATOR SKIP DECORATION");

            return;
        }
        CDIOTelHttpRequestDecorator delegate = DELEGATE;
        if (delegate == null) {
            System.out.println("XXXXXXXXXXXXXXXXXXXXXXXXX DECORATOR 2222");
            synchronized (LOCK) {
                delegate = DELEGATE;
                if (delegate == null) {
                    System.out.println("XXXXXXXXXXXXXXXXXXXXXXXXX DECORATOR 3333");
                    delegate = Arc.container()
                            .instance(CDIOTelHttpRequestDecorator.class)
                            .get();

                    if (delegate == null) {
                        throw new IllegalStateException("CDIOtelHttpRequestDecorator bean could not be found.");
                    }
                    DELEGATE = delegate;
                }
            }
        }
        delegate.decorate(decoratedBuilder, workflowContext, taskContext);
    }
}
