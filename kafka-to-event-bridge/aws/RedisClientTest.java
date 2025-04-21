package com.santander.sov.epppaym.sovepppaym01pymt0028v1gms.infra.client;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for RedisClient class
 */
@ExtendWith(MockitoExtension.class)
public class RedisClientTest {

    @Mock
    private RedisTemplate<String, Object> redisTemplate;
    
    @Mock
    private ValueOperations<String, Object> valueOperations;
    
    @InjectMocks
    private RedisClient redisClient;
    
    private final String testKey = "aws-credentials";
    private final long expirationTime = 3600;
    private final String partitionIdentifier = "HZMS-007-kafka-session";
    private final String partitionIdentifierAWS1 = "HZMS-007-kafka-session-aws1";
    
    @BeforeEach
    void setUp() {
        // Set values for properties using ReflectionTestUtils
        ReflectionTestUtils.setField(redisClient, "expirationTime", expirationTime);
        ReflectionTestUtils.setField(redisClient, "partitionIdentifier", partitionIdentifier);
        ReflectionTestUtils.setField(redisClient, "partitionIdentifierAWS1", partitionIdentifierAWS1);
        
        // Setup mock for redisTemplate.opsForValue()
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
    }
    
    @Test
    void storeAwsCredentials_forAws1_storesWithCorrectKeyAndExpiration() {
        // Arrange
        Map<String, Object> credentials = new HashMap<>();
        credentials.put("accessKey", "testAccessKey");
        credentials.put("secretKey", "testSecretKey");
        
        String expectedRedisKey = partitionIdentifierAWS1 + ":" + testKey;
        
        // Act
        redisClient.storeAwsCredentials(testKey, credentials, true);
        
        // Assert
        verify(valueOperations).set(eq(expectedRedisKey), eq(credentials));
        verify(redisTemplate).expire(eq(expectedRedisKey), eq(expirationTime), eq(TimeUnit.SECONDS));
    }
    
    @Test
    void storeAwsCredentials_forAws2_storesWithCorrectKeyAndExpiration() {
        // Arrange
        Map<String, Object> credentials = new HashMap<>();
        credentials.put("accessKey", "testAccessKey");
        credentials.put("secretKey", "testSecretKey");
        
        String expectedRedisKey = partitionIdentifier + ":" + testKey;
        
        // Act
        redisClient.storeAwsCredentials(testKey, credentials, false);
        
        // Assert
        verify(valueOperations).set(eq(expectedRedisKey), eq(credentials));
        verify(redisTemplate).expire(eq(expectedRedisKey), eq(expirationTime), eq(TimeUnit.SECONDS));
    }
    
    @Test
    void getAwsCredentials_forAws1_retrievesWithCorrectKey() {
        // Arrange
        Map<String, Object> expectedCredentials = new HashMap<>();
        expectedCredentials.put("accessKey", "testAccessKey");
        expectedCredentials.put("secretKey", "testSecretKey");
        
        String expectedRedisKey = partitionIdentifierAWS1 + ":" + testKey;
        when(valueOperations.get(eq(expectedRedisKey))).thenReturn(expectedCredentials);
        
        // Act
        Map<String, Object> result = redisClient.getAwsCredentials(testKey, true);
        
        // Assert
        verify(valueOperations).get(eq(expectedRedisKey));
        assertEquals(expectedCredentials, result);
    }
    
    @Test
    void getAwsCredentials_forAws2_retrievesWithCorrectKey() {
        // Arrange
        Map<String, Object> expectedCredentials = new HashMap<>();
        expectedCredentials.put("accessKey", "testAccessKey");
        expectedCredentials.put("secretKey", "testSecretKey");
        
        String expectedRedisKey = partitionIdentifier + ":" + testKey;
        when(valueOperations.get(eq(expectedRedisKey))).thenReturn(expectedCredentials);
        
        // Act
        Map<String, Object> result = redisClient.getAwsCredentials(testKey, false);
        
        // Assert
        verify(valueOperations).get(eq(expectedRedisKey));
        assertEquals(expectedCredentials, result);
    }
    
    @Test
    void getAwsCredentials_whenKeyNotFound_returnsNull() {
        // Arrange
        String expectedRedisKey = partitionIdentifier + ":" + testKey;
        when(valueOperations.get(eq(expectedRedisKey))).thenReturn(null);
        
        // Act
        Map<String, Object> result = redisClient.getAwsCredentials(testKey, false);
        
        // Assert
        verify(valueOperations).get(eq(expectedRedisKey));
        assertNull(result);
    }
    
    @Test
    void hasCredentials_whenKeyExists_returnsTrue() {
        // Arrange
        String expectedRedisKey = partitionIdentifierAWS1 + ":" + testKey;
        when(redisTemplate.hasKey(eq(expectedRedisKey))).thenReturn(Boolean.TRUE);
        
        // Act
        boolean result = redisClient.hasCredentials(testKey, true);
        
        // Assert
        verify(redisTemplate).hasKey(eq(expectedRedisKey));
        assertTrue(result);
    }
    
    @Test
    void hasCredentials_whenKeyDoesNotExist_returnsFalse() {
        // Arrange
        String expectedRedisKey = partitionIdentifierAWS1 + ":" + testKey;
        when(redisTemplate.hasKey(eq(expectedRedisKey))).thenReturn(Boolean.FALSE);
        
        // Act
        boolean result = redisClient.hasCredentials(testKey, true);
        
        // Assert
        verify(redisTemplate).hasKey(eq(expectedRedisKey));
        assertFalse(result);
    }
    
    @Test
    void hasCredentials_whenNullReturned_returnsFalse() {
        // Arrange
        String expectedRedisKey = partitionIdentifierAWS1 + ":" + testKey;
        when(redisTemplate.hasKey(eq(expectedRedisKey))).thenReturn(null);
        
        // Act
        boolean result = redisClient.hasCredentials(testKey, true);
        
        // Assert
        verify(redisTemplate).hasKey(eq(expectedRedisKey));
        assertFalse(result);
    }
    
    @Test
    void deleteCredentials_forAws1_deletesWithCorrectKey() {
        // Arrange
        String expectedRedisKey = partitionIdentifierAWS1 + ":" + testKey;
        
        // Act
        redisClient.deleteCredentials(testKey, true);
        
        // Assert
        verify(redisTemplate).delete(eq(expectedRedisKey));
    }
    
    @Test
    void deleteCredentials_forAws2_deletesWithCorrectKey() {
        // Arrange
        String expectedRedisKey = partitionIdentifier + ":" + testKey;
        
        // Act
        redisClient.deleteCredentials(testKey, false);
        
        // Assert
        verify(redisTemplate).delete(eq(expectedRedisKey));
    }
}