package com.core.order_api.filter;

import com.core.order_common.util.IdGenerator;
import com.core.order_common.util.TraceContext;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * HTTP Filter that establishes MDC Trace ID context and logs incoming REST requests.
 */
@Slf4j
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class TraceLoggingFilter extends OncePerRequestFilter {

    public static final String X_REQUEST_ID = "X-Request-ID";

    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain
    ) throws ServletException, IOException {
        String requestId = request.getHeader(X_REQUEST_ID);
        if (requestId == null || requestId.isBlank()) {
            requestId = IdGenerator.nextSimpleUuid();
        }

        TraceContext.setRequestId(requestId);
        response.setHeader(X_REQUEST_ID, requestId);

        long startTime = System.currentTimeMillis();
        String method = request.getMethod();
        String uri = request.getRequestURI();

        log.info("[HTTP Inbound] {} {} -> RequestId: [{}]", method, uri, requestId);

        try {
            filterChain.doFilter(request, response);
        } finally {
            long duration = System.currentTimeMillis() - startTime;
            log.info("[HTTP Outbound] {} {} -> Status: [{}], Duration: [{}ms], RequestId: [{}]",
                    method, uri, response.getStatus(), duration, requestId);
            TraceContext.clear();
        }
    }
}
