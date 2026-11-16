package com.oms.service;

import com.oms.domain.ProcessedMessage;
import com.oms.repository.ProcessedMessageRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.UUID;

/** Consumer-side de-duplication (at-least-once delivery -> effectively-once processing). Call inside the handler's transaction. */
@Slf4j
@Service
@RequiredArgsConstructor
public class InboxService {
    private final ProcessedMessageRepository repo;

    /** @return true the first time this (topic,eventId) is seen. */
    public boolean firstTime(String topic, UUID eventId) {
        String id = topic + ":" + eventId;
        if (repo.existsById(id)) return false;
        repo.save(new ProcessedMessage(id));
        return true;
    }
}
