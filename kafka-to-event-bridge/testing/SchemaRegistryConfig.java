package com.example.kafka.config;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class SchemaRegistryConfig {

    @Value("${spring.kafka.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Bean
    public SchemaRegistryClient schemaRegistryClient() {
        RestService restService = new RestService(schemaRegistryUrl);
        Map<String, Object> configs = new HashMap<>();
        
        // Si se necesita autenticación para el Schema Registry
        // configs.put("basic.auth.credentials.source", "USER_INFO");
        // configs.put("basic.auth.user.info", "username:password");
        
        return new CachedSchemaRegistryClient(
                restService, 
                1000, // Capacidad de caché
                Collections.singletonList(new io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer<>()), 
                configs
        );
    }
}
