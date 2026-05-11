package io.quarkiverse.flow.it.otel;

import org.junit.jupiter.api.Test;

import io.quarkiverse.flow.it.otel.util.IndexedSpanInfo;
import io.quarkus.test.junit.QuarkusIntegrationTest;

@QuarkusIntegrationTest
class EmitTaskIT extends OTelBaseIT {

    private static final String EMIT_TASK = "do/0/emitTask";

    @Override
    String workflowName() {
        return "otel-emit-task";
    }

    @Override
    String workVersion() {
        return "1.0.0";
    }

    @Test
    void producedSpans() {
        IndexedSpanInfo indexedSpanInfo = executeAndGetSpans(2);
        assertThatHasParent(indexedSpanInfo, IndexedSpanInfo.TaskSpanKey.from(EMIT_TASK), workflowParentSpan());
    }
}
