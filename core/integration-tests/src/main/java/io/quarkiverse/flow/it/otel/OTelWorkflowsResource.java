package io.quarkiverse.flow.it.otel;

import java.util.Map;

import jakarta.inject.Inject;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.quarkiverse.flow.Flow;
import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;

@Path("otel-workflows")
public class OTelWorkflowsResource {

    @Inject
    @Identifier("otel:otel-set-task:1.0.0")
    Flow setTaskFlow;

    @Inject
    @Identifier("otel:otel-do-task:1.0.0")
    Flow doTaskFlow;

    @Path("otel-set-task")
    @POST
    public Uni<Map<String, Object>> postSetTaskFlow(ObjectNode input) {
        return doCall(setTaskFlow, input);
    }

    @Path("otel-do-task")
    @POST
    public Uni<Map<String, Object>> postDoTaskFlow(ObjectNode input) {
        return doCall(doTaskFlow, input);
    }

    Uni<Map<String, Object>> doCall(Flow flow, ObjectNode input) {
        return flow.startInstance(input)
                .onItem()
                .transform(result -> result.asMap().orElseThrow());
    }
}
