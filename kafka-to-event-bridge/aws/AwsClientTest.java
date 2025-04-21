package com.santander.sov.epppaym.sovepppaym01pymt0028v1gms.infra.client;

import com.santander.sov.epppaym.sovepppaym01pymt0028v1gms.util.AwsIamAuthGenerate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpEntity;
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
@ExtendWith(MockitoExtension.class)
public class AwsClientTest {

    @Mock
    private RedisClient redisClient;
    
    @Mock
    private AwsIamAuthGenerate awsIamAuthGenerate;
    
    @Mock
    private RestTemplate restTemplate;
    
    // Spy on the class to test to allow partial mocking
    @InjectMocks
    private AwsClient awsClient;
    
    private final String regionAws1 = "us-east-1";
    private final String regionAws2 = "us-east-2";
    private final String eventBridgeHostAws1 = "events.us-east-1.amazonaws.com";
    private final String eventBridgeHostAws2 = "events.us-east-2.amazonaws.com";
    
    @BeforeEach
    void setUp() {
        // Set only the necessary fields for the tests
        ReflectionTestUtils.setField(awsClient, "regionAws1", regionAws1);
        ReflectionTestUtils.setField(awsClient, "regionAws2", regionAws2);
        ReflectionTestUtils.setField(awsClient, "eventBridgeHostAws1", eventBridgeHostAws1);
        ReflectionTestUtils.setField(awsClient, "eventBridgeHostAws2", eventBridgeHostAws2);
        
        // Mock static or utility methods that don't need to vary between tests
        when(awsIamAuthGenerate.createEventBridgeHeaders(any(), any(), any(), any()))
            .thenReturn(new HttpHeaders());
    }
    
    @Test
    void sendEventToAws1_withValidCachedCredentials_returnsTrue() {
        // Arrange
        Map<String, Object> validCredentials = new HashMap<>();
        validCredentials.put("accessKey", "testAccessKey");
        validCredentials.put("secretKey", "testSecretKey");
        validCredentials.put("sessionToken", "testSessionToken");
        validCredentials.put("expiration", String.valueOf(Instant.now().plusSeconds(3600).toEpochMilli()));
        
        when(redisClient.getAwsCredentials(any(), eq(true))).thenReturn(validCredentials);
        
        // CRITICAL: Mock the RestTemplate to return a non-null response
        ResponseEntity<String> mockResponse = mock(ResponseEntity.class);
        when(restTemplate.postForEntity(anyString(), any(), eq(String.class)))
            .thenReturn(mockResponse);
        
        // Act
        boolean result = awsClient.sendEventToAws1(new HashMap<>());
        
        // Assert
        assertTrue(result);
    }
    
    @Test
    void sendEventToAws1_withExpiredCredentials_generatesNewCredentials() {
        // Arrange
        // 1. Create expired credentials
        Map<String, Object> expiredCredentials = new HashMap<>();
        expiredCredentials.put("accessKey", "expiredKey");
        expiredCredentials.put("secretKey", "expiredSecret");
        expiredCredentials.put("expiration", String.valueOf(Instant.now().minusSeconds(60).toEpochMilli()));
        
        // 2. Create new credentials that will be generated
        Map<String, Object> newCredentials = new HashMap<>();
        newCredentials.put("accessKey", "newKey");
        newCredentials.put("secretKey", "newSecret");
        newCredentials.put("sessionToken", "newToken");
        newCredentials.put("expiration", String.valueOf(Instant.now().plusSeconds(3600).toEpochMilli()));
        
        // 3. Setup mocks
        when(redisClient.getAwsCredentials(any(), eq(true))).thenReturn(expiredCredentials);
        when(awsIamAuthGenerate.createIamRequestBody(any(), any(), any(), any())).thenReturn(new HashMap<>());
        when(awsIamAuthGenerate.decodeBase64(any())).thenReturn("dummy".getBytes());
        when(awsIamAuthGenerate.getAwsCredentialsFromIam(any(), any(), any())).thenReturn(newCredentials);
        
        // CRITICAL: Mock the RestTemplate to return a non-null response
        ResponseEntity<String> mockResponse = mock(ResponseEntity.class);
        when(restTemplate.postForEntity(anyString(), any(), eq(String.class)))
            .thenReturn(mockResponse);
        
        // Act
        boolean result = awsClient.sendEventToAws1(new HashMap<>());
        
        // Assert
        assertTrue(result);
    }
    
    @Test
    void sendEventToAws1_withRestTemplateException_returnsFalse() {
        // Arrange
        Map<String, Object> validCredentials = new HashMap<>();
        validCredentials.put("accessKey", "testAccessKey");
        validCredentials.put("secretKey", "testSecretKey");
        validCredentials.put("expiration", String.valueOf(Instant.now().plusSeconds(3600).toEpochMilli()));
        
        when(redisClient.getAwsCredentials(any(), eq(true))).thenReturn(validCredentials);
        
        // Mock RestTemplate to throw exception
        when(restTemplate.postForEntity(anyString(), any(), eq(String.class)))
            .thenThrow(new RuntimeException("Test exception"));
        
        // Act
        boolean result = awsClient.sendEventToAws1(new HashMap<>());
        
        // Assert
        assertFalse(result);
    }
    
    @Test
    void sendEventToAws2_withValidCachedCredentials_returnsTrue() {
        // Arrange
        Map<String, Object> validCredentials = new HashMap<>();
        validCredentials.put("accessKey", "testAccessKey");
        validCredentials.put("secretKey", "testSecretKey");
        validCredentials.put("expiration", String.valueOf(Instant.now().plusSeconds(3600).toEpochMilli()));
        
        when(redisClient.getAwsCredentials(any(), eq(false))).thenReturn(validCredentials);
        
        // CRITICAL: Mock the RestTemplate to return a non-null response
        ResponseEntity<String> mockResponse = mock(ResponseEntity.class);
        when(restTemplate.postForEntity(anyString(), any(), eq(String.class)))
            .thenReturn(mockResponse);
        
        // Act
        boolean result = awsClient.sendEventToAws2(new HashMap<>());
        
        // Assert
        assertTrue(result);
    }
    
    @Test
    void sendEventToAws2_withNoCredentialsInCache_generatesNewCredentials() {
        // Arrange
        // 1. New credentials that will be generated
        Map<String, Object> newCredentials = new HashMap<>();
        newCredentials.put("accessKey", "newKey");
        newCredentials.put("secretKey", "newSecret");
        newCredentials.put("sessionToken", "newToken");
        newCredentials.put("expiration", String.valueOf(Instant.now().plusSeconds(3600).toEpochMilli()));
        
        // 2. Setup mocks
        when(redisClient.getAwsCredentials(any(), eq(false))).thenReturn(null);
        when(awsIamAuthGenerate.createIamRequestBody(any(), any(), any(), any())).thenReturn(new HashMap<>());
        when(awsIamAuthGenerate.decodeBase64(any())).thenReturn("dummy".getBytes());
        when(awsIamAuthGenerate.getAwsCredentialsFromIam(any(), any(), any())).thenReturn(newCredentials);
        
        // CRITICAL: Mock the RestTemplate to return a non-null response
        ResponseEntity<String> mockResponse = mock(ResponseEntity.class);
        when(restTemplate.postForEntity(anyString(), any(), eq(String.class)))
            .thenReturn(mockResponse);
        
        // Act
        boolean result = awsClient.sendEventToAws2(new HashMap<>());
        
        // Assert
        assertTrue(result);
    }
    
    @Test
    void sendEventToAws2_withIamException_usesMockCredentials() {
        // Arrange
        when(redisClient.getAwsCredentials(any(), eq(false))).thenReturn(null);
        when(awsIamAuthGenerate.createIamRequestBody(any(), any(), any(), any())).thenReturn(new HashMap<>());
        when(awsIamAuthGenerate.decodeBase64(any())).thenReturn("dummy".getBytes());
        
        // Mock IAM to throw exception
        when(awsIamAuthGenerate.getAwsCredentialsFromIam(any(), any(), any()))
            .thenThrow(new RuntimeException("IAM Error"));
        
        // CRITICAL: Mock the RestTemplate to return a non-null response
        ResponseEntity<String> mockResponse = mock(ResponseEntity.class);
        when(restTemplate.postForEntity(anyString(), any(), eq(String.class)))
            .thenReturn(mockResponse);
        
        // Act
        boolean result = awsClient.sendEventToAws2(new HashMap<>());
        
        // Assert
        assertTrue(result);
    }
}