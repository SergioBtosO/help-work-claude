package com.empresa.connector.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConfig {

    @Value("${kafka.bootstrap-servers}")
    private List<String> bootstrapServers;
    
    @Value("${kafka.security.username}")
    private String username;
    
    @Value("${kafka.security.password}")
    private String password;
    
    @Value("${kafka.consumer.group-id}")
    private String groupId;
    
    @Value("${kafka.consumer.max-polling-interval:6000}")
    private Integer maxPollingInterval;
    
    @Value("${kafka.consumer.retry-backoff-timeout:100}")
    private Integer retryBackoffTimeout;
    
    @Value("${kafka.consumer.reconnection.attempts:2}")
    private Integer reconnectionAttempts;
    
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", bootstrapServers));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollingInterval);
        
        // SASL Authentication configuration
        if (username != null && !username.isEmpty() && password != null && !password.isEmpty()) {
            String jaasConfig = "org.apache.kafka.common.security.plain.PlainLoginModule required "
                    + "username=\"" + username + "\" "
                    + "password=\"" + password + "\";";
            props.put("sasl.jaas.config", jaasConfig);
            props.put("security.protocol", "SASL_PLAINTEXT");
            props.put("sasl.mechanism", "PLAIN");
        }

        return new DefaultKafkaConsumerFactory<>(props);
    }
    
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = 
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        
        // Error handler with backoff strategy
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                new FixedBackOff(retryBackoffTimeout, reconnectionAttempts.longValue()));
        factory.setCommonErrorHandler(errorHandler);
        
        // Manual acknowledgment mode
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        
        return factory;
    }
}