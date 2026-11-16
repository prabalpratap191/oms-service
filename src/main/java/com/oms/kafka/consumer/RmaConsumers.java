package com.oms.kafka.consumer;

import com.oms.kafka.KafkaGroups;
import com.oms.kafka.KafkaTopics;
import com.oms.kafka.events.Events;
import com.oms.service.RmaService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/** Reverse-logistics responses + refund/credit-memo acknowledgements. */
@Slf4j
@Component
@Transactional
@RequiredArgsConstructor
public class RmaConsumers {
    private final InboundSupport in;
    private final RmaService rma;

    @KafkaListener(topics = KafkaTopics.LOGISTICS_OMS_RETURN_PICKUP_RESULT, groupId = KafkaGroups.OMS_RMA)
    public void onReturnPickup(ConsumerRecord<String, String> r) {
        in.handle(r, Events.ReturnPickupResult.class, e -> rma.onReturnPickupResult(e.payload()));
    }

    @KafkaListener(topics = KafkaTopics.WMS_OMS_RETURN_INSPECTION_RESULT, groupId = KafkaGroups.OMS_RMA)
    public void onReturnInspection(ConsumerRecord<String, String> r) {
        in.handle(r, Events.ReturnInspectionResult.class, e -> rma.onReturnInspectionResult(e.payload()));
    }

    @KafkaListener(topics = KafkaTopics.PAYMENT_OMS_PAYMENT_REFUND_RESULT, groupId = KafkaGroups.OMS_RMA)
    public void onRefund(ConsumerRecord<String, String> r) {
        in.handle(r, Events.PaymentRefundResult.class, e -> {
            if ("RMA".equals(e.payload().referenceType())) rma.onRefundResult(e.payload());
            else log.info("Refund {} for order {} ({}) success={}", e.payload().refundId(), e.payload().orderId(), e.payload().referenceType(), e.payload().success());
        });
    }

    @KafkaListener(topics = KafkaTopics.FINANCE_OMS_CREDIT_MEMO_ACK, groupId = KafkaGroups.OMS_RMA)
    public void onCreditMemoAck(ConsumerRecord<String, String> r) {
        in.handle(r, Events.CreditMemoAck.class, e -> log.info("Credit memo {} acknowledged success={}", e.payload().creditMemoId(), e.payload().success()));
    }
}
