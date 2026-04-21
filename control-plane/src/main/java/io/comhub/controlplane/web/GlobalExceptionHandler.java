package io.comhub.controlplane.web;

import io.comhub.controlplane.domain.ConfigPublishException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Maps controller-surface exceptions to RFC 7807 {@link ProblemDetail} responses.
 *
 * <p>Validation errors become {@code 400 Bad Request} with a {@code fieldErrors} extension so
 * the forthcoming {@code /config} UI can render inline field-local messages. Configuration
 * publish failures become {@code 503 Service Unavailable}, signalling that the write path to
 * {@code comhub.config.v1} is not currently durable and that local cache state is preserved.
 *
 * @author Roman Hadiuchko
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ProblemDetail handleValidation(MethodArgumentNotValidException ex) {
        Map<String, String> fieldErrors = new LinkedHashMap<>();
        ex.getBindingResult().getFieldErrors().forEach(error ->
                fieldErrors.put(error.getField(), error.getDefaultMessage()));

        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.BAD_REQUEST,
                "Request validation failed");
        problem.setTitle("Invalid request");
        problem.setProperty("fieldErrors", fieldErrors);
        return problem;
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ProblemDetail handleMalformedBody(HttpMessageNotReadableException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.BAD_REQUEST,
                "Request body could not be parsed");
        problem.setTitle("Malformed request");
        return problem;
    }

    @ExceptionHandler(ConfigPublishException.class)
    public ProblemDetail handleConfigPublishFailure(ConfigPublishException ex) {
        log.warn("Config topic publish failure: {}", ex.getMessage());

        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.SERVICE_UNAVAILABLE,
                "Configuration change could not be published to the config topic; local state is unchanged.");
        problem.setTitle("Config topic unavailable");
        return problem;
    }
}
