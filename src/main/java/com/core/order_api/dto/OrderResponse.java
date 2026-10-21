package com.core.order_api.dto;

import com.core.order_model.entity.OrderEntity;
import com.core.order_model.entity.OrderStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderResponse {

    private Long id;
    private String orderNumber;
    private String customerId;
    private BigDecimal totalAmount;
    private OrderStatus status;
    private String notes;
    private Long version;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

    public static OrderResponse fromEntity(OrderEntity entity) {
        if (entity == null) {
            return null;
        }
        return OrderResponse.builder()
                .id(entity.getId())
                .orderNumber(entity.getOrderNumber())
                .customerId(entity.getCustomerId())
                .totalAmount(entity.getTotalAmount())
                .status(entity.getStatus())
                .notes(entity.getNotes())
                .version(entity.getVersion())
                .createdAt(entity.getCreatedAt())
                .updatedAt(entity.getUpdatedAt())
                .build();
    }
}
