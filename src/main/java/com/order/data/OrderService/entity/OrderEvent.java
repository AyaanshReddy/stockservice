package com.order.data.OrderService.entity;

import com.shiping.data.ShipingService.dto.enums.OrderStatus;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;

@Document(collection = "OrderEvent")
public class OrderEvent {
    @Id
    private String id;
    private String orderId;
    private OrderStatus status;  // CREATED, CONFIRMED
    private String details;
    private LocalDateTime eventTimestamp;

    @Override
    public String toString() {
        return "OrderEvent{" +
                "id='" + id + '\'' +
                ", orderId='" + orderId + '\'' +
                ", status=" + status +
                ", details='" + details + '\'' +
                ", eventTimestamp=" + eventTimestamp +
                '}';
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public OrderStatus getStatus() {
        return status;
    }

    public void setStatus(OrderStatus status) {
        this.status = status;
    }

    public String getDetails() {
        return details;
    }

    public void setDetails(String details) {
        this.details = details;
    }

    public LocalDateTime getEventTimestamp() {
        return eventTimestamp;
    }

    public void setEventTimestamp(LocalDateTime eventTimestamp) {
        this.eventTimestamp = eventTimestamp;
    }

    public OrderEvent() {
    }

    public OrderEvent(String orderId, OrderStatus status, String details, LocalDateTime eventTimestamp) {
        this.orderId = orderId;
        this.status = status;
        this.details = details;
        this.eventTimestamp = eventTimestamp;
    }
}
