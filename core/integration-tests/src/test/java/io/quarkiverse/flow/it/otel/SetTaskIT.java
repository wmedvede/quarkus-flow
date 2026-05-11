package io.quarkiverse.flow.it.otel;

import org.junit.jupiter.api.Test;

import io.quarkiverse.flow.it.otel.util.IndexedSpanInfo;
import io.quarkus.test.junit.QuarkusIntegrationTest;

@QuarkusIntegrationTest
class SetTaskIT extends OTelBaseIT {

    private static final String SET_TASK = "do/0/setTask";

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
        IndexedSpanInfo indexedSpanInfo = executeAndGetSpans(2);
        assertThatHasParent(indexedSpanInfo, IndexedSpanInfo.TaskSpanKey.from(SET_TASK), workflowParentSpan());
    }
}
