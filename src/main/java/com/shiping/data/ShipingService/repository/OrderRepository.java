package com.shiping.data.ShipingService.repository;


import com.order.data.OrderService.entity.OrderEvent;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface OrderRepository extends MongoRepository<OrderEvent, String> {
}
