package com.core.order_api.service;

import com.core.order_api.dto.CreateOrderRequest;
import com.core.order_api.dto.OrderResponse;
import com.core.order_common.enums.OutboxStatus;
import com.core.order_common.exception.AppException;
import com.core.order_common.exception.CommonErrorCode;
import com.core.order_common.util.IdGenerator;
import com.core.order_common.util.JsonUtils;
import com.core.order_common.util.TraceContext;
import com.core.order_model.entity.OrderEntity;
import com.core.order_model.entity.OrderStatus;
import com.core.order_model.entity.OutboxEntity;
import com.core.order_model.event.BaseEvent;
import com.core.order_model.event.KafkaTopics;
import com.core.order_model.event.OrderCreatedPayload;
import com.core.order_model.event.OrderStatusChangedPayload;
import com.core.order_model.repository.OrderRepository;
import com.core.order_model.repository.OutboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

/**
 * Order Application Service using Transactional Outbox Pattern to guarantee 100% atomic consistency.
 * Prevents the Dual-Writer anomaly by saving Entity and Outbox record in the same database transaction.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxRepository outboxRepository;
    private final ApplicationEventPublisher applicationEventPublisher;

    /**
     * Atomically creates an Order and inserts an Outbox event in the same ACID transaction.
     */
    @Transactional
    public OrderResponse createOrder(CreateOrderRequest request) {
        String orderNumber = IdGenerator.nextOrderNumber();
        log.info("[OrderService] Creating order [{}] atomically with Outbox event", orderNumber);

        // 1. Save Order JPA Entity (Optimistic Lock version = 0)
        OrderEntity orderEntity = OrderEntity.builder()
                .orderNumber(orderNumber)
                .customerId(request.getCustomerId())
                .totalAmount(request.getTotalAmount())
                .status(OrderStatus.CREATED)
                .notes(request.getNotes())
                .version(0L)
                .build();
        OrderEntity saved = orderRepository.save(orderEntity);

        // 2. Prepare Kafka event payload & envelope
        OrderCreatedPayload payload = OrderCreatedPayload.builder()
                .orderNumber(saved.getOrderNumber())
                .customerId(saved.getCustomerId())
                .totalAmount(saved.getTotalAmount())
                .notes(saved.getNotes())
                .build();

        BaseEvent<OrderCreatedPayload> event = BaseEvent.of(
                KafkaTopics.EVENT_TYPE_ORDER_CREATED,
                saved.getOrderNumber(),
                payload
        );

        // 3. Save Outbox Entity in the SAME database transaction
        OutboxEntity outbox = OutboxEntity.builder()
                .eventId(event.getEventId())
                .aggregateType("ORDER")
                .aggregateId(saved.getOrderNumber())
                .eventType(KafkaTopics.EVENT_TYPE_ORDER_CREATED)
                .topic(KafkaTopics.ORDER_EVENTS_TOPIC)
                .payload(JsonUtils.toJson(event))
                .traceId(TraceContext.getTraceId())
                .status(OutboxStatus.PENDING)
                .retryCount(0)
                .build();

        outboxRepository.save(outbox);
        log.info("[OrderService] Order [{}] and Outbox event [{}] saved atomically.", orderNumber, outbox.getEventId());

        // 4. Publish Spring internal event (caught AFTER_COMMIT to dispatch immediately)
        applicationEventPublisher.publishEvent(outbox);

        return OrderResponse.fromEntity(saved);
    }

    /**
     * Cancels an order with optimistic locking and writes cancellation event to Outbox atomically.
     */
    @Transactional
    public OrderResponse cancelOrder(String orderNumber, String reason) {
        log.info("[OrderService] Cancelling order [{}]", orderNumber);

        OrderEntity order = orderRepository.findByOrderNumber(orderNumber)
                .orElseThrow(() -> new AppException(CommonErrorCode.NOT_FOUND, "Order not found: " + orderNumber));

        if (order.getStatus() == OrderStatus.CANCELLED) {
            return OrderResponse.fromEntity(order);
        }

        OrderStatus oldStatus = order.getStatus();
        order.setStatus(OrderStatus.CANCELLED);
        order.setUpdatedAt(LocalDateTime.now());
        OrderEntity updated = orderRepository.save(order);

        // Prepare Status Changed Event
        OrderStatusChangedPayload payload = OrderStatusChangedPayload.builder()
                .orderNumber(orderNumber)
                .oldStatus(oldStatus)
                .newStatus(OrderStatus.CANCELLED)
                .reason(reason)
                .build();

        BaseEvent<OrderStatusChangedPayload> event = BaseEvent.of(
                KafkaTopics.EVENT_TYPE_ORDER_STATUS_CHANGED,
                orderNumber,
                payload
        );

        // Save to Outbox in same transaction
        OutboxEntity outbox = OutboxEntity.builder()
                .eventId(event.getEventId())
                .aggregateType("ORDER")
                .aggregateId(orderNumber)
                .eventType(KafkaTopics.EVENT_TYPE_ORDER_STATUS_CHANGED)
                .topic(KafkaTopics.ORDER_EVENTS_TOPIC)
                .payload(JsonUtils.toJson(event))
                .traceId(TraceContext.getTraceId())
                .status(OutboxStatus.PENDING)
                .retryCount(0)
                .build();

        outboxRepository.save(outbox);
        applicationEventPublisher.publishEvent(outbox);

        return OrderResponse.fromEntity(updated);
    }
}
