package com.shiping.data.ShipingService.config;


import com.order.data.OrderService.entity.OrderEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConfig {

    /*@Bean
    public ProducerFactory<String, OrderEvent> producerFactory()
    {

        // Creating a Map
        Map<String, Object> config = new HashMap<>();

        // Adding Configuration

        // 127.0.0.1:9092 is the default port number for
        // kafka
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "127.0.0.1:9092");
        config.put(
                ProducerConfig.KEY_ SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringDeserializer.class);
        config.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.springframework.kafka.support.serializer.JsonSerializer .class);

        return new DefaultKafkaProducerFactory<>(config);
    }

    // Annotation
    // Method
    @Bean
    public KafkaTemplate kafkaTemplate()
    {
        return new KafkaTemplate(producerFactory());
    }*/
    @Bean
    public ConsumerFactory<String, OrderEvent> consumerFactory()
    {

        // Creating a Map of string-object pairs
        Map<String, Object> config = new HashMap<>();

        // Adding the Configuration
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "127.0.0.1:9092");
        config.put(ConsumerConfig.GROUP_ID_CONFIG,
                "user-group");
        config.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        config.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        /*config.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ErrorHandlingDeserializer.class);*/
        //org.springframework.kafka.support.serializer.JsonDeserializer
       config.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                JsonDeserializer.class);
        return new DefaultKafkaConsumerFactory<String, OrderEvent>(config);
    }

    // Creating a Listener
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderEvent>
    concurrentKafkaListenerContainerFactory()
    {
        ConcurrentKafkaListenerContainerFactory<
                String, OrderEvent> factory
                = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }

   /* @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, AccountRequest>> listenerContainerFactory(
            ConsumerFactory<String, AccountRequest> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, AccountRequest> listenerContainerFactory =
                new ConcurrentKafkaListenerContainerFactory<>();
        listenerContainerFactory.setConsumerFactory(consumerFactory);
        return listenerContainerFactory;
    }*/
}
