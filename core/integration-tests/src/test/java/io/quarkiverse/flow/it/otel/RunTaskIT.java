package io.quarkiverse.flow.it.otel;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import io.quarkiverse.flow.it.otel.util.IndexedSpanInfo;
import io.quarkus.test.junit.QuarkusIntegrationTest;

@QuarkusIntegrationTest
@EnabledOnOs(value = OS.LINUX)
class RunTaskIT extends OTelBaseIT {

    private static final String RUN_TASK = "do/0/runTask";

    @Override
    String workflowName() {
        return "otel-run-task";
    }

    @Override
    String workVersion() {
        return "1.0.0";
    }

    @Test
    void producedSpans() {
        IndexedSpanInfo indexedSpanInfo = executeAndGetSpans(2);
        assertThatHasParent(indexedSpanInfo, IndexedSpanInfo.TaskSpanKey.from(RUN_TASK), workflowParentSpan());
    }
}
