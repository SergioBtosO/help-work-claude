package com.empresa.connector.service;

import com.empresa.connector.handler.KafkaToEventBridgeHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaListenerService {

    private final KafkaToEventBridgeHandler handler;

    @KafkaListener(
        topics = "${kafka.consumer.topic-pattern}",
        groupId = "${kafka.consumer.group-id}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void receivePaymentEvent(
            @Payload String payload,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) Integer partition,
            @Header(KafkaHeaders.OFFSET) Long offset,
            Acknowledgment ack) {
        
        Instant startTime = Instant.now();
        
        try {
            // Log INPUT attributes
            log.info("Message received - Topic: {}, Partition: {}, Offset: {}", 
                    topic, partition, offset);
            
            // Log INPUT payload
            log.info("Payload received: {}", payload);
            
            // Process message with handler
            handler.processEvent(payload, topic, partition, offset);
            
            // Acknowledge successful processing
            ack.acknowledge();
            
            // Log OUTPUT execution time
            long executionTime = Instant.now().toEpochMilli() - startTime.toEpochMilli();
            log.info("Total execution time: {} ms", executionTime);
            
        } catch (Exception e) {
            log.error("Error processing Kafka message: {}", e.getMessage(), e);
            // No acknowledgment to retry according to retry policy
        }
    }
}