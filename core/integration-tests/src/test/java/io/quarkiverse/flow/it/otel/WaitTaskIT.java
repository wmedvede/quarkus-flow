package io.quarkiverse.flow.it.otel;

import org.junit.jupiter.api.Test;

import io.quarkiverse.flow.it.otel.util.IndexedSpanInfo;
import io.quarkus.test.junit.QuarkusIntegrationTest;

@QuarkusIntegrationTest
class WaitTaskIT extends OTelBaseIT {

    private static final String WAIT_TASK1 = "do/0/waitTask1";
    private static final String SET_TASK = "do/1/setTask";
    private static final String WAIT_TASK2 = "do/2/waitTask2";

    @Override
    String workflowName() {
        return "otel-wait-task";
    }

    @Test
    void producedSpans() {
        IndexedSpanInfo indexedSpanInfo = executeAndGetSpans(4);
        assertThatHasParent(indexedSpanInfo, IndexedSpanInfo.TaskSpanKey.from(WAIT_TASK1), workflowParentSpan());
        assertThatHasParent(indexedSpanInfo, IndexedSpanInfo.TaskSpanKey.from(SET_TASK), workflowParentSpan());
        assertThatHasParent(indexedSpanInfo, IndexedSpanInfo.TaskSpanKey.from(WAIT_TASK2), workflowParentSpan());
    }
}
