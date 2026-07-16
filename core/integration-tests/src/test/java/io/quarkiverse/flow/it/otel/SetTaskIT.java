package io.quarkiverse.flow.it.otel;

import org.junit.jupiter.api.Test;

import io.quarkiverse.flow.it.otel.util.IndexedSpanInfo;
import io.quarkus.test.junit.QuarkusIntegrationTest;

@QuarkusIntegrationTest
class SetTaskIT extends OTelBaseIT {

    @Override
    String workflowName() {
        return "otel-set-task";
    }

    @Override
    String workVersion() {
        return "1.0.0";
    }

    @Test
    void producedSpans() {
        IndexedSpanInfo indexedSpanInfo = executeAndGetSpans(1);
        assertThatHasParent(indexedSpanInfo, IndexedSpanInfo.TaskSpanKey.from("do/0/setTask", 1, 0, false),
                IndexedSpanInfo.WorkflowSpanKey.from("otel-set-task", "1.0.0"));
    }
}
