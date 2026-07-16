package io.quarkiverse.flow.it.otel;

import org.junit.jupiter.api.Test;

import io.quarkiverse.flow.it.otel.util.IndexedSpanInfo;
import io.quarkus.test.junit.QuarkusIntegrationTest;

@QuarkusIntegrationTest
class DoTaskIT extends OTelBaseIT {
    @Override
    String workflowName() {
        return "otel-do-task";
    }

    @Test
    void producedSpans() {
        IndexedSpanInfo indexedSpanInfo = executeAndGetSpans(5);

        assertThatHasParent(indexedSpanInfo, IndexedSpanInfo.TaskSpanKey.from("do/0/doTask/do", 1, 0, false),
                IndexedSpanInfo.WorkflowSpanKey.from("otel-do-task", "1.0.0"));

        assertThatHasParent(indexedSpanInfo, IndexedSpanInfo.TaskSpanKey.from("do/0/doTask/do/0/set1", 1, 0, false),
                IndexedSpanInfo.TaskSpanKey.from("do/0/doTask/do", 1, 0, false));

        assertThatHasParent(indexedSpanInfo, IndexedSpanInfo.TaskSpanKey.from("do/0/doTask/do/1/set2", 1, 0, false),
                IndexedSpanInfo.TaskSpanKey.from("do/0/doTask/do", 1, 0, false));

        assertThatHasParent(indexedSpanInfo, IndexedSpanInfo.TaskSpanKey.from("do/0/doTask/do/2/set3", 1, 0, false),
                IndexedSpanInfo.TaskSpanKey.from("do/0/doTask/do", 1, 0, false));

    }

}
