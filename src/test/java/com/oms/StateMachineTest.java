package com.oms;

import com.oms.common.InvalidStateException;
import com.oms.domain.OrderStatus;
import com.oms.service.IdempotencyService;
import com.oms.statemachine.OrderStateMachine;
import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import static org.junit.jupiter.api.Assertions.*;

class StateMachineTest {
    private final OrderStateMachine sm = new OrderStateMachine();

    @Test void happyPath() {
        assertTrue(sm.canTransition(OrderStatus.PENDING, OrderStatus.SOURCED));
        assertTrue(sm.canTransition(OrderStatus.READY_TO_SHIP, OrderStatus.SHIPPED));
    }
    @Test void illegalTransitionsRejected() {
        assertThrows(InvalidStateException.class, () -> sm.assertAllowed(OrderStatus.PENDING, OrderStatus.DELIVERED));
        assertThrows(InvalidStateException.class, () -> sm.assertAllowed(OrderStatus.CANCELLED, OrderStatus.PENDING));
        assertFalse(sm.canTransition(OrderStatus.DELIVERED, OrderStatus.CANCELLED));
    }
    @Test void idempotencyKeyIsDeterministic() {
        String a = IdempotencyService.key("t", "c", "cart", new BigDecimal("10.5"));
        assertEquals(a, IdempotencyService.key("t", "c", "cart", new BigDecimal("10.50")));
        assertNotEquals(a, IdempotencyService.key("t", "c", "cart2", new BigDecimal("10.50")));
    }
}
