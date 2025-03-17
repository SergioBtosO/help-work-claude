package com.example.kafka.config;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class SchemaRegistryConfig {

    @Value("${schema.registry.url}")
    private String schemaRegistryUrl;
    
    @Value("${schema.registry.username:}")
    private String username;
    
    @Value("${schema.registry.password:}")
    private String password;
    
    @Value("${schema.registry.max.cached.schemas:1000}")
    private int maxCachedSchemas;
    
    @Bean
    public SchemaRegistryClient schemaRegistryClient() {
        RestService restService = new RestService(schemaRegistryUrl);
        Map<String, Object> configs = new HashMap<>();
        
        // Configuración de autenticación si se proporciona
        if (username != null && !username.isEmpty() && password != null && !password.isEmpty()) {
            configs.put("basic.auth.credentials.source", "USER_INFO");
            configs.put("basic.auth.user.info", username + ":" + password);
        }
        
        // Usar serializador AVRO (pero luego convertiremos a JSON)
        return new CachedSchemaRegistryClient(
                restService, 
                maxCachedSchemas, 
                Collections.singletonList(new KafkaAvroSerializer()), 
                configs
        );
    }
}
