package com.oms.repository;

import com.oms.domain.OutboxEvent;
import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;
import java.util.*;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {
    @Query(value = "select * from outbox_events where status = 'PENDING' order by created_at limit :n for update skip locked", nativeQuery = true)
    List<OutboxEvent> lockPending(@Param("n") int n);
}
