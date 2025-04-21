package com.santander.sov.epppaym.sovepppaym01pymt0028v1gms.infra.client;

import com.santander.sov.epppaym.sovepppaym01pymt0028v1gms.util.AwsIamAuthGenerate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
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
    
    @InjectMocks
    private AwsClient awsClient;
    
    private final String regionAws1 = "us-east-1";
    private final String regionAws2 = "us-east-2";
    private final String eventBridgeHostAws1 = "events.us-east-1.amazonaws.com";
    private final String eventBridgeHostAws2 = "events.us-east-2.amazonaws.com";
    
    @BeforeEach
    void setUp() {
        // Set only essential fields
        ReflectionTestUtils.setField(awsClient, "regionAws1", regionAws1);
        ReflectionTestUtils.setField(awsClient, "regionAws2", regionAws2);
        ReflectionTestUtils.setField(awsClient, "eventBridgeHostAws1", eventBridgeHostAws1);
        ReflectionTestUtils.setField(awsClient, "eventBridgeHostAws2", eventBridgeHostAws2);
        
        // Mock ResponseEntity for RestTemplate
        ResponseEntity<String> mockResponse = mock(ResponseEntity.class);
        
        // Common mocks that apply to all tests - use lenient() to avoid "unnecessary stubbing" errors
        lenient().when(awsIamAuthGenerate.createEventBridgeHeaders(any(), any(), any(), any()))
            .thenReturn(new HttpHeaders());
        lenient().when(restTemplate.postForEntity(anyString(), any(), eq(String.class)))
            .thenReturn(mockResponse);
    }
    
    @Test
    void sendEventToAws1_withValidCachedCredentials_returnsTrue() {
        // Arrange
        Map<String, Object> validCredentials = new HashMap<>();
        validCredentials.put("accessKey", "testAccessKey");
        validCredentials.put("secretKey", "testSecretKey");
        validCredentials.put("sessionToken", "testSessionToken");
        validCredentials.put("expiration", String.valueOf(Instant.now().plusSeconds(3600).toEpochMilli()));
        
        // Only mock what this test specifically needs
        when(redisClient.getAwsCredentials(any(), eq(true))).thenReturn(validCredentials);
        
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
        
        // Override the common mock for this specific test
        when(restTemplate.postForEntity(contains(eventBridgeHostAws1), any(), eq(String.class)))
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
        
        // Act
        boolean result = awsClient.sendEventToAws2(new HashMap<>());
        
        // Assert
        assertTrue(result);
    }
    
    // Test with separate mock configurations to avoid any unwanted stub interactions
    
    @Test
    void sendEventToAws1_withExpiredCredentials_generatesNewCredentials() {
        // Setup a completely separate test with its own mocks
        AwsClient localAwsClient = new AwsClient(redisClient, awsIamAuthGenerate, restTemplate);
        
        // Set field values
        ReflectionTestUtils.setField(localAwsClient, "regionAws1", regionAws1);
        ReflectionTestUtils.setField(localAwsClient, "eventBridgeHostAws1", eventBridgeHostAws1);
        
        // Create expired credentials
        Map<String, Object> expiredCredentials = new HashMap<>();
        expiredCredentials.put("accessKey", "expiredKey");
        expiredCredentials.put("secretKey", "expiredSecret");
        expiredCredentials.put("expiration", String.valueOf(Instant.now().minusSeconds(60).toEpochMilli()));
        
        // Create new credentials
        Map<String, Object> newCredentials = new HashMap<>();
        newCredentials.put("accessKey", "newKey");
        newCredentials.put("secretKey", "newSecret");
        newCredentials.put("sessionToken", "newToken");
        newCredentials.put("expiration", String.valueOf(Instant.now().plusSeconds(3600).toEpochMilli()));
        
        // Configure mocks specifically for this test
        when(redisClient.getAwsCredentials(any(), eq(true))).thenReturn(expiredCredentials);
        when(awsIamAuthGenerate.getAwsCredentialsFromIam(any(), any(), any())).thenReturn(newCredentials);
        when(awsIamAuthGenerate.createEventBridgeHeaders(any(), any(), any(), any())).thenReturn(new HttpHeaders());
        when(restTemplate.postForEntity(any(), any(), eq(String.class))).thenReturn(mock(ResponseEntity.class));
        
        // Act
        boolean result = localAwsClient.sendEventToAws1(new HashMap<>());
        
        // Assert
        assertTrue(result);
    }
    
    @Test
    void sendEventToAws2_withNoCredentialsInCache_generatesNewCredentials() {
        // Setup a completely separate test with its own mocks
        AwsClient localAwsClient = new AwsClient(redisClient, awsIamAuthGenerate, restTemplate);
        
        // Set field values
        ReflectionTestUtils.setField(localAwsClient, "regionAws2", regionAws2);
        ReflectionTestUtils.setField(localAwsClient, "eventBridgeHostAws2", eventBridgeHostAws2);
        
        // New credentials that will be generated
        Map<String, Object> newCredentials = new HashMap<>();
        newCredentials.put("accessKey", "newKey");
        newCredentials.put("secretKey", "newSecret");
        newCredentials.put("sessionToken", "newToken");
        newCredentials.put("expiration", String.valueOf(Instant.now().plusSeconds(3600).toEpochMilli()));
        
        // Configure mocks specifically for this test
        when(redisClient.getAwsCredentials(any(), eq(false))).thenReturn(null);
        when(awsIamAuthGenerate.getAwsCredentialsFromIam(any(), any(), any())).thenReturn(newCredentials);
        when(awsIamAuthGenerate.createEventBridgeHeaders(any(), any(), any(), any())).thenReturn(new HttpHeaders());
        when(restTemplate.postForEntity(any(), any(), eq(String.class))).thenReturn(mock(ResponseEntity.class));
        
        // Act
        boolean result = localAwsClient.sendEventToAws2(new HashMap<>());
        
        // Assert
        assertTrue(result);
    }
    
    @Test
    void sendEventToAws2_withIamException_usesMockCredentials() {
        // Setup a completely separate test with its own mocks
        AwsClient localAwsClient = new AwsClient(redisClient, awsIamAuthGenerate, restTemplate);
        
        // Set field values
        ReflectionTestUtils.setField(localAwsClient, "regionAws2", regionAws2);
        ReflectionTestUtils.setField(localAwsClient, "eventBridgeHostAws2", eventBridgeHostAws2);
        
        // Configure mocks specifically for this test
        when(redisClient.getAwsCredentials(any(), eq(false))).thenReturn(null);
        when(awsIamAuthGenerate.getAwsCredentialsFromIam(any(), any(), any()))
            .thenThrow(new RuntimeException("IAM Error"));
        when(awsIamAuthGenerate.createEventBridgeHeaders(any(), any(), any(), any())).thenReturn(new HttpHeaders());
        when(restTemplate.postForEntity(any(), any(), eq(String.class))).thenReturn(mock(ResponseEntity.class));
        
        // Act
        boolean result = localAwsClient.sendEventToAws2(new HashMap<>());
        
        // Assert
        assertTrue(result);
    }
}