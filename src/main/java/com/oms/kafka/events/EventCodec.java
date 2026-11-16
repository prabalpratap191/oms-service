package com.oms.kafka.events;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class EventCodec {
    private final ObjectMapper mapper;

    public String serialize(Object o) {
        try { return mapper.writeValueAsString(o); }
        catch (JsonProcessingException e) { throw new IllegalStateException("Cannot serialize event", e); }
    }

    public <T> EventEnvelope<T> parse(String json, Class<T> payloadType) {
        try {
            return mapper.readValue(json, mapper.getTypeFactory().constructParametricType(EventEnvelope.class, payloadType));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Malformed event: " + e.getOriginalMessage(), e);
        }
    }

    public <T> T convert(Object o, Class<T> type) { return mapper.convertValue(o, type); }
}
