package com.oms.domain;
import jakarta.persistence.*;
import lombok.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.*;

@Entity @Table(name = "processed_messages") @Getter @Setter @NoArgsConstructor
public class ProcessedMessage {
    @Id private String id;                 // topic:eventId (inbox / consumer de-duplication)
    private Instant processedAt = Instant.now();
    public ProcessedMessage(String id) { this.id = id; }
}
