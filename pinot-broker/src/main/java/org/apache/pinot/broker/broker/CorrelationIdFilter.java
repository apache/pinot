package org.apache.pinot.broker.broker;

import java.io.IOException;
import java.util.UUID;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.ext.Provider;
import org.apache.logging.log4j.ThreadContext;

@Provider
public class CorrelationIdFilter implements ContainerRequestFilter, ContainerResponseFilter {

    public static final String CORRELATION_ID_HEADER = "X-Correlation-Id";

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        // Retrieve or generate correlation ID
        String correlationId = requestContext.getHeaderString(CORRELATION_ID_HEADER);
        if (correlationId == null) {
            correlationId = UUID.randomUUID().toString();
        }

        // Set correlation ID to be accessible by the rest of the application
        ThreadContext.put(CORRELATION_ID_HEADER, correlationId);
        requestContext.getHeaders().add(CORRELATION_ID_HEADER, correlationId);
    }

    @Override
    public void filter(ContainerRequestContext requestContext,
                       ContainerResponseContext responseContext) throws IOException {
        // Add correlation ID to response headers
        String correlationId = ThreadContext.get(CORRELATION_ID_HEADER);
        if (correlationId != null) {
            responseContext.getHeaders().add(CORRELATION_ID_HEADER, correlationId);
        }

        // Clean up the ThreadContext
        ThreadContext.remove(CORRELATION_ID_HEADER);
    }
}
