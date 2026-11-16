package com.oms.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class MdcLoggingFilter extends OncePerRequestFilter {

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return false;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        String requestId = UUID.randomUUID().toString();
        String path = request.getRequestURI();
        String method = request.getMethod();

        MDC.put("requestId", requestId);
        MDC.put("httpMethod", method);
        MDC.put("path", path);
        MDC.put("clientIp", request.getRemoteAddr());
        MDC.put("tenantId", request.getHeader("X-Tenant-Id") != null ? request.getHeader("X-Tenant-Id") : "n/a");
        MDC.put("actorType", request.getHeader("X-Actor-Type") != null ? request.getHeader("X-Actor-Type") : "n/a");
        MDC.put("actorId", request.getHeader("X-Actor-Id") != null ? request.getHeader("X-Actor-Id") : "n/a");

        response.setHeader("X-Request-Id", requestId);

        try {
            filterChain.doFilter(request, response);
        } finally {
            MDC.remove("requestId");
            MDC.remove("httpMethod");
            MDC.remove("path");
            MDC.remove("clientIp");
            MDC.remove("tenantId");
            MDC.remove("actorType");
            MDC.remove("actorId");
        }
    }
}
