package com.core.order_api.controller;

import com.core.order_api.dto.CreateOrderRequest;
import com.core.order_api.dto.OrderResponse;
import com.core.order_api.service.OrderService;
import com.core.order_common.dto.ApiResponse;
import com.core.order_common.exception.AppException;
import com.core.order_common.exception.CommonErrorCode;
import com.core.order_model.document.OrderEventDocument;
import com.core.order_model.entity.OrderEntity;
import com.core.order_model.repository.OrderEventMongoRepository;
import com.core.order_model.repository.OrderRepository;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/api/v1/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;
    private final OrderRepository orderRepository;
    private final OrderEventMongoRepository eventMongoRepository;

    /**
     * Creates a new order using the Transactional Outbox Pattern.
     * Prevents Dual-Writer issues by storing Order and Outbox event in the same atomic database commit.
     */
    @PostMapping
    public ResponseEntity<ApiResponse<OrderResponse>> createOrder(
            @Valid @RequestBody CreateOrderRequest request
    ) {
        OrderResponse response = orderService.createOrder(request);
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(ApiResponse.success("Order created successfully", response));
    }

    /**
     * Retrieves an order by order number from JPA repository.
     */
    @GetMapping("/{orderNumber}")
    public ResponseEntity<ApiResponse<OrderResponse>> getOrderByNumber(@PathVariable String orderNumber) {
        OrderEntity order = orderRepository.findByOrderNumber(orderNumber)
                .orElseThrow(() -> new AppException(CommonErrorCode.NOT_FOUND, "Order not found: " + orderNumber));

        return ResponseEntity.ok(ApiResponse.success(OrderResponse.fromEntity(order)));
    }

    /**
     * Retrieves all event history / audit logs for an order from MongoDB Event Store.
     */
    @GetMapping("/{orderNumber}/events")
    public ResponseEntity<ApiResponse<List<OrderEventDocument>>> getOrderEvents(@PathVariable String orderNumber) {
        List<OrderEventDocument> events = eventMongoRepository.findByAggregateIdOrderByCreatedAtDesc(orderNumber);
        return ResponseEntity.ok(ApiResponse.success(events));
    }

    /**
     * Cancels an order with optimistic locking race condition detection.
     */
    @PutMapping("/{orderNumber}/cancel")
    public ResponseEntity<ApiResponse<OrderResponse>> cancelOrder(
            @PathVariable String orderNumber,
            @RequestParam(defaultValue = "User requested cancellation") String reason
    ) {
        OrderResponse response = orderService.cancelOrder(orderNumber, reason);
        return ResponseEntity.ok(ApiResponse.success("Order cancelled", response));
    }
}
