package io.quarkiverse.flow.it.otel;

import org.junit.jupiter.api.Test;

import io.quarkiverse.flow.it.otel.util.IndexedSpanInfo;
import io.quarkus.test.junit.QuarkusIntegrationTest;

@QuarkusIntegrationTest
class RaiseTaskIT extends OTelBaseIT {

    private static final String SET_BEFORE_RAISE = "do/0/setBeforeRaise";
    private static final String RAISE_TASK = "do/1/raiseTask";

    @Override
    String workflowName() {
        return "otel-raise-task";
    }

    @Override
    int statusCode() {
        return 500;
    }

    @Test
    void producedSpans() {
        IndexedSpanInfo indexedSpanInfo = executeAndGetSpans(3);
        assertThatHasParent(indexedSpanInfo, IndexedSpanInfo.TaskSpanKey.from(SET_BEFORE_RAISE), workflowParentSpan());
        assertThatHasParent(indexedSpanInfo, IndexedSpanInfo.TaskSpanKey.from(RAISE_TASK), workflowParentSpan());
    }

}
