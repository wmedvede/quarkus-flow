package io.quarkiverse.flow.it.otel;

import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.MediaType;

import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.SpanData;

@ApplicationScoped
@Path("otel-span-exporter")
public class OTelSpanExporterResource {

    @Inject
    InMemorySpanExporter exporter;

    @Path("spans")
    @GET
    @jakarta.ws.rs.Consumes(MediaType.APPLICATION_JSON)
    @jakarta.ws.rs.Produces(MediaType.APPLICATION_JSON)
    public List<SpanData> getSpans() {
        List<SpanData> spans = exporter.getFinishedSpanItems();
        System.out.println("VAMOOOOOOOOOOOOOOOO: " + spans.size());
        return spans;
    }

    @ApplicationScoped
    static class InMemorySpanExporterProducer {
        @Produces
        @Singleton
        InMemorySpanExporter inMemorySpanExporter() {
            return InMemorySpanExporter.create();
        }
    }
}
