package com.core.order_api.service;

import com.core.order_common.enums.OutboxStatus;
import com.core.order_common.util.JsonUtils;
import com.core.order_common.util.TraceContext;
import com.core.order_model.entity.OutboxEntity;
import com.core.order_model.event.BaseEvent;
import com.core.order_model.repository.OutboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

/**
 * Generic Transactional Outbox Helper Service.
 *
 * Domain-agnostic component that enqueues any domain event into the outbox table
 * within the active database transaction, guaranteeing atomic consistency and preventing
 * the Dual-Writer problem.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class GenericOutboxService {

    private final OutboxRepository outboxRepository;
    private final ApplicationEventPublisher applicationEventPublisher;

    /**
     * Enqueues a generic domain event into the Outbox table.
     * Fires a Spring TransactionalEventListener to attempt instant publishing upon commit.
     *
     * @param aggregateType The entity/aggregate type (e.g., "Order", "User", "Payment", "Device")
     * @param aggregateId   The business identifier of the aggregate
     * @param eventType     The event action (e.g., "CREATED", "UPDATED", "CANCELLED")
     * @param topic         Target Kafka topic
     * @param payload       The payload DTO (will be wrapped in BaseEvent<T>)
     * @return Saved OutboxEntity
     */
    @Transactional
    public <T> OutboxEntity enqueueEvent(
            String aggregateType,
            String aggregateId,
            String eventType,
            String topic,
            T payload
    ) {
        BaseEvent<T> envelope = BaseEvent.of(eventType, aggregateId, payload);
        String jsonPayload = JsonUtils.toJson(envelope);

        OutboxEntity outbox = OutboxEntity.builder()
                .eventId(envelope.getEventId())
                .aggregateType(aggregateType)
                .aggregateId(aggregateId)
                .eventType(eventType)
                .topic(topic)
                .payload(jsonPayload)
                .status(OutboxStatus.PENDING)
                .retryCount(0)
                .requestId(envelope.getRequestId() != null ? envelope.getRequestId() : TraceContext.getRequestId())
                .createdAt(LocalDateTime.now())
                .build();

        OutboxEntity saved = outboxRepository.save(outbox);
        log.info("[Outbox] Enqueued event [{}] for aggregate [{}:{}] to topic [{}]",
                envelope.getEventId(), aggregateType, aggregateId, topic);

        // Fire local Spring event. The listener with @TransactionalEventListener(phase = AFTER_COMMIT)
        // will immediately send it to Kafka as soon as DB transaction commits.
        applicationEventPublisher.publishEvent(saved);

        return saved;
    }
}
