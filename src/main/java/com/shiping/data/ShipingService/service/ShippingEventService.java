package com.shiping.data.ShipingService.service;

import com.shiping.data.ShipingService.dto.enums.OrderStatus;
import com.order.data.OrderService.entity.OrderEvent;
import com.shiping.data.ShipingService.repository.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
public class ShippingEventService {

Logger logger = LoggerFactory.getLogger(ShippingEventService.class);
    @Autowired
    OrderRepository repository;

    @KafkaListener(groupId = "user-group",topics = "order-events",containerFactory = "concurrentKafkaListenerContainerFactory")
    public void consumOrderEvent(OrderEvent event){
        logger.info("@@@@@@@@@@@@@@@consumer calling@@@@@@@@@@@@@@@@@"+event);
        if (event.getStatus().equals(OrderStatus.CONFIRMED))
            shipOrder(event.getOrderId());
    }

    // Ship the order
    public void shipOrder(String orderId) {
        logger.info("%%%%%%%%%%%%%%%%shipOrder%%%%%%%%%%%%%"+orderId);
        OrderEvent orderEvent = new OrderEvent(orderId, OrderStatus.SHIPPED,"Order Shipped Successfully..", LocalDateTime.now());
        repository.save(orderEvent);
    }

    // Deliver the order
    public void deliverOrder(String orderId) {
        logger.info("#################3 deliverOrder $$$$$$$$$$$$$$$$$"+orderId);
        OrderEvent orderEvent = new OrderEvent(orderId, OrderStatus.DELIVERED,"Order Delivered Successfully..", LocalDateTime.now());
        repository.save(orderEvent);

    }
}
