package com.oms.api;

import com.oms.common.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.http.*;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.*;
import java.util.stream.Collectors;

@Slf4j
@RestControllerAdvice
public class ApiExceptionHandler {
    private static ProblemDetail pd(HttpStatus s, String detail) { return ProblemDetail.forStatusAndDetail(s, detail); }

    @ExceptionHandler(NotFoundException.class) ProblemDetail notFound(NotFoundException e) { return pd(HttpStatus.NOT_FOUND, e.getMessage()); }
    @ExceptionHandler({InvalidStateException.class, ConflictException.class}) ProblemDetail conflict(RuntimeException e) { return pd(HttpStatus.CONFLICT, e.getMessage()); }
    @ExceptionHandler(ForbiddenException.class) ProblemDetail forbidden(ForbiddenException e) { return pd(HttpStatus.FORBIDDEN, e.getMessage()); }
    @ExceptionHandler(IllegalArgumentException.class) ProblemDetail bad(IllegalArgumentException e) { return pd(HttpStatus.BAD_REQUEST, e.getMessage()); }
    @ExceptionHandler(OptimisticLockingFailureException.class) ProblemDetail locked(OptimisticLockingFailureException e) { return pd(HttpStatus.CONFLICT, "Concurrent modification, please retry"); }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ProblemDetail invalid(MethodArgumentNotValidException e) {
        return pd(HttpStatus.BAD_REQUEST, e.getBindingResult().getFieldErrors().stream()
            .map(f -> f.getField() + " " + f.getDefaultMessage()).collect(Collectors.joining("; ")));
    }

    @ExceptionHandler(Exception.class)
    ProblemDetail other(Exception e) { log.error("Unhandled", e); return pd(HttpStatus.INTERNAL_SERVER_ERROR, "Internal error"); }
}
