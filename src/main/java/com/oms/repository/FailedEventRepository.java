package com.oms.repository;

import com.oms.domain.*;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.*;

public interface FailedEventRepository extends JpaRepository<FailedEvent, UUID> {
    List<FailedEvent> findByStatusOrderByCreatedAtAsc(FailedEventStatus status);
}
