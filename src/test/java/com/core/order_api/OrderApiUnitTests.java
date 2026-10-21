package com.core.order_api;

import com.core.order_api.dto.CreateOrderRequest;
import com.core.order_api.dto.OrderResponse;
import com.core.order_model.entity.OrderEntity;
import com.core.order_model.entity.OrderStatus;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class OrderApiUnitTests {

    @Test
    void testOrderResponseMapping() {
        OrderEntity entity = OrderEntity.builder()
                .id(1L)
                .orderNumber("ORD-12345")
                .customerId("CUST-99")
                .totalAmount(BigDecimal.valueOf(99.99))
                .status(OrderStatus.CREATED)
                .version(0L)
                .build();

        OrderResponse response = OrderResponse.fromEntity(entity);
        assertNotNull(response);
        assertEquals("ORD-12345", response.getOrderNumber());
        assertEquals(BigDecimal.valueOf(99.99), response.getTotalAmount());
    }

    @Test
    void testCreateOrderRequest() {
        CreateOrderRequest request = CreateOrderRequest.builder()
                .customerId("CUST-10")
                .totalAmount(BigDecimal.valueOf(50.0))
                .notes("Urgent delivery")
                .build();

        assertEquals("CUST-10", request.getCustomerId());
        assertEquals("Urgent delivery", request.getNotes());
    }
}
