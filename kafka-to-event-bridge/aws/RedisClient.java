package com.santander.sov.epppaym.sovepppaym01pymt0028v1gms.infra.client;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component
public class RedisClient {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    
    @Value("${spring.data.redis.expiration-time}")
    private long expirationTime;
    
    @Value("${spring.data.redis.partition.identifier}")
    private String partitionIdentifier;
    
    @Value("${spring.data.redis.partition.identifier.AWS1}")
    private String partitionIdentifierAWS1;

    // Método para almacenar credenciales de AWS en Redis
    public void storeAwsCredentials(String key, Map<String, Object> credentials, boolean isAws1) {
        String redisKey = createRedisKey(key, isAws1);
        redisTemplate.opsForValue().set(redisKey, credentials);
        redisTemplate.expire(redisKey, expirationTime, TimeUnit.SECONDS);
    }

    // Método para recuperar credenciales de AWS de Redis
    @SuppressWarnings("unchecked")
    public Map<String, Object> getAwsCredentials(String key, boolean isAws1) {
        String redisKey = createRedisKey(key, isAws1);
        Object credentials = redisTemplate.opsForValue().get(redisKey);
        return credentials != null ? (Map<String, Object>) credentials : null;
    }
    
    // Método para crear la clave de Redis basada en el identificador de partición
    private String createRedisKey(String key, boolean isAws1) {
        return isAws1 ? 
               partitionIdentifierAWS1 + ":" + key : 
               partitionIdentifier + ":" + key;
    }
    
    // Método para verificar si hay credenciales en caché
    public boolean hasCredentials(String key, boolean isAws1) {
        String redisKey = createRedisKey(key, isAws1);
        return Boolean.TRUE.equals(redisTemplate.hasKey(redisKey));
    }
    
    // Método para eliminar credenciales
    public void deleteCredentials(String key, boolean isAws1) {
        String redisKey = createRedisKey(key, isAws1);
        redisTemplate.delete(redisKey);
    }
}