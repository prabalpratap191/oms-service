package com.oms.statemachine;

import com.oms.common.TransitionTable;
import com.oms.domain.RmaStatus;
import org.springframework.stereotype.Component;
import static com.oms.domain.RmaStatus.*;

/** REQUESTED -> APPROVED -> PICKUP_SCHEDULED -> QC_INSPECTION -> REFUNDED | EXCHANGED (REJECTED at any pre-refund gate). */
@Component
public class RmaStateMachine extends TransitionTable<RmaStatus> {
    public RmaStateMachine() {
        super(RmaStatus.class);
        allow(REQUESTED, APPROVED, REJECTED);
        allow(APPROVED, PICKUP_SCHEDULED, REJECTED);
        allow(PICKUP_SCHEDULED, QC_INSPECTION);
        allow(QC_INSPECTION, REFUNDED, EXCHANGED, REJECTED);
    }
}
