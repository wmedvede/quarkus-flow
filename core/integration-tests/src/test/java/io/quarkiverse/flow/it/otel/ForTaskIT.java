package io.quarkiverse.flow.it.otel;

import static io.quarkiverse.flow.it.otel.util.Utils.OBJECT_MAPPER;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.quarkiverse.flow.it.otel.util.IndexedSpanInfo;
import io.quarkus.test.junit.QuarkusIntegrationTest;

@QuarkusIntegrationTest
class ForTaskIT extends OTelBaseIT {
    private static final String FOR_TASK = "do/0/forTask/do";
    private static final String SET_TASK1 = "do/0/forTask/do/0/setTask1";
    private static final String SET_TASK2 = "do/0/forTask/do/1/setTask2";

    @Override
    String workflowName() {
        return "otel-for-task";
    }

    @Test
    void producedSpansNoIterations() {
        doProducedSpans(0);
    }

    @Test
    void producedSpansOneIterations() {
        doProducedSpans(1);
    }

    @Test
    void producedSpansNIterations() {
        doProducedSpans(5);
    }

    void doProducedSpans(int iterations) {
        ObjectNode input = buildInput(iterations);
        IndexedSpanInfo indexedSpanInfo = executeAndGetSpans(1 + 1 + iterations * 2, input.toString());
        assertThatHasParent(indexedSpanInfo, IndexedSpanInfo.TaskSpanKey.from(FOR_TASK), workflowParentSpan());
        for (int i = 1; i <= iterations; i++) {
            assertThatHasParent(indexedSpanInfo, IndexedSpanInfo.TaskSpanKey.from(SET_TASK1, i),
                    IndexedSpanInfo.TaskSpanKey.from(FOR_TASK));
            assertThatHasParent(indexedSpanInfo, IndexedSpanInfo.TaskSpanKey.from(SET_TASK2, i),
                    IndexedSpanInfo.TaskSpanKey.from(FOR_TASK));
        }
    }

    private static ObjectNode buildInput(int size) {
        ObjectNode input = OBJECT_MAPPER.createObjectNode();
        ArrayNode items = input.putArray("forItems");
        for (int i = 0; i < size; i++) {
            items.addObject().put("item" + i, "itemValue" + i);
        }
        return input;
    }
}
