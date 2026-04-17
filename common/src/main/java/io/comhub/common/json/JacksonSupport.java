package io.comhub.common.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Provides a shared, pre-configured ObjectMapper for canonical event and configuration JSON handling.
 *
 * @author Roman Hadiuchko
 */
public final class JacksonSupport {

    private static final ObjectMapper SHARED = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private JacksonSupport() {
    }

    public static ObjectMapper sharedObjectMapper() {
        return SHARED;
    }
}
