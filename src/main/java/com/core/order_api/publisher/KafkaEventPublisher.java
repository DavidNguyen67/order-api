package com.core.order_api.publisher;

import com.core.order_common.enums.EventStatus;
import com.core.order_common.enums.OutboxStatus;
import com.core.order_common.util.TraceContext;
import com.core.order_model.document.OrderEventDocument;
import com.core.order_model.entity.OutboxEntity;
import com.core.order_model.repository.OrderEventMongoRepository;
import com.core.order_model.repository.OutboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;

/**
 * Kafka Event Publisher decoupled via Transactional Outbox Pattern.
 * Publishes events only after the database transaction has committed.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaEventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final OutboxRepository outboxRepository;
    private final OrderEventMongoRepository eventMongoRepository;

    /**
     * Publishes an existing OutboxEntity to Kafka and updates Outbox + Mongo audit store upon ACK.
     */
    public CompletableFuture<SendResult<String, String>> publishOutbox(OutboxEntity outbox) {
        String topic = outbox.getTopic();
        String eventId = outbox.getEventId();
        String aggregateId = outbox.getAggregateId();
        String requestId = outbox.getRequestId() != null ? outbox.getRequestId() : TraceContext.getRequestId();

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, aggregateId, outbox.getPayload());
        record.headers().add(new RecordHeader(TraceContext.REQUEST_ID_KEY, requestId.getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader(TraceContext.CORRELATION_ID_KEY, requestId.getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader("eventType", outbox.getEventType().getBytes(StandardCharsets.UTF_8)));

        long startTime = System.currentTimeMillis();
        log.info("[OutboxPublisher] Dispatching Outbox event -> Topic: [{}], EventId: [{}], AggregateId: [{}]",
                topic, eventId, aggregateId);

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(record);

        future.whenComplete((result, ex) -> {
            long duration = System.currentTimeMillis() - startTime;
            if (ex == null) {
                var metadata = result.getRecordMetadata();
                log.info("[OutboxPublisher] Successfully published -> Topic: [{}], Partition: [{}], Offset: [{}], EventId: [{}], Duration: [{}ms]",
                        metadata.topic(), metadata.partition(), metadata.offset(), eventId, duration);

                // 1. Update Outbox record in JPA
                outbox.setStatus(OutboxStatus.PUBLISHED);
                outbox.setProcessedAt(LocalDateTime.now());
                outboxRepository.save(outbox);

                // 2. Replicate to MongoDB Event Store for audit log / query model
                syncToMongo(outbox, metadata.partition(), metadata.offset());
            } else {
                log.error("[OutboxPublisher] Failed to publish Outbox event [{}]: {}. Will be retried by Quartz Job.",
                        eventId, ex.getMessage(), ex);

                outbox.setRetryCount(outbox.getRetryCount() + 1);
                outbox.setLastError(ex.getMessage());
                outboxRepository.save(outbox);
            }
        });

        return future;
    }

    private void syncToMongo(OutboxEntity outbox, int partition, long offset) {
        try {
            OrderEventDocument mongoDoc = OrderEventDocument.builder()
                    .eventId(outbox.getEventId())
                    .eventType(outbox.getEventType())
                    .aggregateId(outbox.getAggregateId())
                    .topic(outbox.getTopic())
                    .partition(partition)
                    .offset(offset)
                    .payload(outbox.getPayload())
                    .status(EventStatus.PUBLISHED)
                    .requestId(outbox.getRequestId())
                    .createdAt(Instant.now())
                    .processedAt(Instant.now())
                    .build();

            eventMongoRepository.save(mongoDoc);
            log.info("[OutboxPublisher] Replicated Outbox event [{}] to Mongo Event Store", outbox.getEventId());
        } catch (Exception e) {
            log.warn("[OutboxPublisher] Failed to replicate event [{}] to Mongo Event Store: {}",
                    outbox.getEventId(), e.getMessage());
        }
    }
}
