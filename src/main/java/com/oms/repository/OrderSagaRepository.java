package com.oms.repository;

import com.oms.domain.OrderSaga;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.UUID;

public interface OrderSagaRepository extends JpaRepository<OrderSaga, UUID> {}
