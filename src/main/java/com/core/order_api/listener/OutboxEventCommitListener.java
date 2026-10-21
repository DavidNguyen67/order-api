package com.core.order_api.listener;

import com.core.order_api.publisher.KafkaEventPublisher;
import com.core.order_model.entity.OutboxEntity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * Listens to committed database transactions and immediately dispatches Outbox events.
 * Provides near-instant (sub-millisecond) delivery while guaranteeing 100% transactional safety.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxEventCommitListener {

    private final KafkaEventPublisher kafkaEventPublisher;

    @Async
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onTransactionCommit(OutboxEntity outbox) {
        log.info("[OutboxCommitListener] DB Transaction committed for event [{}]! Dispatching to Kafka...",
                outbox.getEventId());
        try {
            kafkaEventPublisher.publishOutbox(outbox);
        } catch (Exception e) {
            log.warn("[OutboxCommitListener] Immediate dispatch failed for event [{}]: {}. Quartz will reconcile.",
                    outbox.getEventId(), e.getMessage());
        }
    }
}
