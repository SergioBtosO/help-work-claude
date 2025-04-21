package com.santander.sov.epppaym.sovepppaym01pymt0028v1gms.infra.client;

import com.santander.sov.epppaym.sovepppaym01pymt0028v1gms.util.AwsIamAuthGenerate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.quality.Strictness;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.client.RestTemplate;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for AwsClient class
 */
public class AwsClientTest {

    @Mock
    private RedisClient redisClient;
    
    @Mock
    private AwsIamAuthGenerate awsIamAuthGenerate;
    
    @Mock
    private RestTemplate restTemplate;
    
    private AwsClient awsClient;
    
    private final String regionAws1 = "us-east-1";
    private final String regionAws2 = "us-east-2";
    private final String eventBridgeHostAws1 = "events.us-east-1.amazonaws.com";
    private final String eventBridgeHostAws2 = "events.us-east-2.amazonaws.com";
    
    @BeforeEach
    void setUp() {
        // Initialize mocks with LENIENT strictness
        MockitoAnnotations.openMocks(
            this, 
            Mockito.withSettings().strictness(Strictness.LENIENT)
        );
        
        // Create the AwsClient instance manually
        awsClient = new AwsClient(redisClient, awsIamAuthGenerate, restTemplate);
        
        // Set minimal required fields
        ReflectionTestUtils.setField(awsClient, "regionAws1", regionAws1);
        ReflectionTestUtils.setField(awsClient, "regionAws2", regionAws2);
        ReflectionTestUtils.setField(awsClient, "eventBridgeHostAws1", eventBridgeHostAws1);
        ReflectionTestUtils.setField(awsClient, "eventBridgeHostAws2", eventBridgeHostAws2);
    }
    
    @Test
    void sendEventToAws1_withValidCachedCredentials_returnsTrue() {
        // 1. Setup only the minimal required mocks
        Map<String, Object> validCredentials = new HashMap<>();
        validCredentials.put("accessKey", "testAccessKey");
        validCredentials.put("secretKey", "testSecretKey");
        validCredentials.put("expiration", String.valueOf(Instant.now().plusSeconds(3600).toEpochMilli()));
        
        when(redisClient.getAwsCredentials(any(), eq(true))).thenReturn(validCredentials);
        when(awsIamAuthGenerate.createEventBridgeHeaders(any(), any(), any(), any())).thenReturn(new HttpHeaders());
        when(restTemplate.postForEntity(any(), any(), any())).thenReturn(ResponseEntity.ok("success"));
        
        // 2. Act
        boolean result = awsClient.sendEventToAws1(new HashMap<>());
        
        // 3. Assert
        assertTrue(result);
    }
    
    @Test
    void sendEventToAws1_withRestTemplateException_returnsFalse() {
        // 1. Setup only the minimal required mocks
        Map<String, Object> validCredentials = new HashMap<>();
        validCredentials.put("accessKey", "testAccessKey");
        validCredentials.put("secretKey", "testSecretKey");
        validCredentials.put("expiration", String.valueOf(Instant.now().plusSeconds(3600).toEpochMilli()));
        
        when(redisClient.getAwsCredentials(any(), eq(true))).thenReturn(validCredentials);
        when(awsIamAuthGenerate.createEventBridgeHeaders(any(), any(), any(), any())).thenReturn(new HttpHeaders());
        when(restTemplate.postForEntity(any(), any(), any())).thenThrow(new RuntimeException("Test exception"));
        
        // 2. Act
        boolean result = awsClient.sendEventToAws1(new HashMap<>());
        
        // 3. Assert
        assertFalse(result);
    }
    
    @Test
    void sendEventToAws2_withValidCachedCredentials_returnsTrue() {
        // 1. Setup only the minimal required mocks
        Map<String, Object> validCredentials = new HashMap<>();
        validCredentials.put("accessKey", "testAccessKey");
        validCredentials.put("secretKey", "testSecretKey");
        validCredentials.put("expiration", String.valueOf(Instant.now().plusSeconds(3600).toEpochMilli()));
        
        when(redisClient.getAwsCredentials(any(), eq(false))).thenReturn(validCredentials);
        when(awsIamAuthGenerate.createEventBridgeHeaders(any(), any(), any(), any())).thenReturn(new HttpHeaders());
        when(restTemplate.postForEntity(any(), any(), any())).thenReturn(ResponseEntity.ok("success"));
        
        // 2. Act
        boolean result = awsClient.sendEventToAws2(new HashMap<>());
        
        // 3. Assert
        assertTrue(result);
    }
}