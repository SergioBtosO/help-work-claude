package com.empresa.connector.service;

import com.empresa.connector.model.AwsCredentials;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;

/**
 * Service interface for AWS authentication
 */
public interface AwsAuthService {
    
    /**
     * Generates secure AWS headers for authentication
     * 
     * @param method HTTP method
     * @param region AWS region
     * @param service AWS service name
     * @param operation AWS operation
     * @param uri Request URI
     * @param queryParams Query parameters
     * @param requestBody Request body
     * @param credentials AWS credentials
     * @return HTTP headers with AWS authentication
     */
    HttpHeaders generateSecureAwsHeaders(String method, 
                                     String region, 
                                     String service, 
                                     String operation, 
                                     String uri, 
                                     String queryParams, 
                                     String requestBody,
                                     AwsCredentials credentials);
    
    /**
     * Sends an authenticated request to AWS
     * 
     * @param url Full URL
     * @param httpMethod HTTP method
     * @param body Request body
     * @param responseType Response type class
     * @param credentials AWS credentials
     * @param region AWS region
     * @param service AWS service name
     * @param operation AWS operation
     * @return Response entity
     */
    <T> ResponseEntity<T> sendAuthenticatedRequest(String url, 
                                            HttpMethod httpMethod, 
                                            Object body, 
                                            Class<T> responseType,
                                            AwsCredentials credentials,
                                            String region,
                                            String service,
                                            String operation);
}

package com.empresa.connector.service.impl;

import com.empresa.connector.config.properties.AwsProperties;
import com.empresa.connector.model.AwsCredentials;
import com.empresa.connector.service.AwsAuthService;
import com.empresa.connector.util.AwsSigner;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class AwsAuthServiceImpl implements AwsAuthService {

    private final AwsProperties awsProperties;
    private final AwsSigner awsSigner;
    private final RestTemplate restTemplate;
    
    @Override
    public HttpHeaders generateSecureAwsHeaders(String method, 
                                          String region, 
                                          String service, 
                                          String operation, 
                                          String uri, 
                                          String queryParams, 
                                          String requestBody,
                                          AwsCredentials credentials) {
        try {
            // Get current date in AWS format
            ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
            String awsDate = now.format(DateTimeFormatter.ofPattern(awsProperties.getDatetimeFormat()));
            String dateStamp = now.format(DateTimeFormatter.ofPattern(awsProperties.getDateFormat()));
            
            // Calculate payload hash
            String payloadHash = awsSigner.hashContent(requestBody);
            
            // Create basic headers map
            Map<String, String> headerMap = new HashMap<>();
            headerMap.put("host", String.format("%s.%s.amazonaws.com", service, region));
            headerMap.put("x-amz-content-sha256", payloadHash);
            headerMap.put("x-amz-date", awsDate);
            
            // Add target if specified
            if (operation != null && !operation.isEmpty()) {
                headerMap.put("x-amz-target", operation);
            }
            
            // Add security token if available
            if (credentials.getSessionToken() != null && !credentials.getSessionToken().isEmpty()) {
                headerMap.put("x-amz-security-token", credentials.getSessionToken());
            }
            
            // Calculate AWS authorization
            String authorization = awsSigner.computeAuthorizationHeader(
                awsDate, 
                dateStamp, 
                region, 
                service,
                method,
                uri,
                queryParams,
                headerMap,
                payloadHash,
                credentials);
                
            // Create HTTP headers
            HttpHeaders headers = new HttpHeaders();
            headers.add("Authorization", authorization);
            headers.add("Content-Type", awsProperties.getContentType());
            
            // Add all headers from the map
            headerMap.forEach(headers::add);
            
            return headers;
        } catch (Exception e) {
            log.error("Error generating AWS headers: {}", e.getMessage(), e);
            throw new RuntimeException("Error generating AWS headers", e);
        }
    }
    
    @Override
    public <T> ResponseEntity<T> sendAuthenticatedRequest(String url, 
                                                    HttpMethod httpMethod, 
                                                    Object body, 
                                                    Class<T> responseType,
                                                    AwsCredentials credentials,
                                                    String region,
                                                    String service,
                                                    String operation) {
        try {
            // Extract URI and query params
            String uri = extractUri(url);
            String queryParams = extractQueryParams(url);
            
            // Convert body to string if not null
            String requestBody = (body != null) ? convertBodyToString(body) : "";
            
            // Generate AWS signed headers
            HttpHeaders headers = generateSecureAwsHeaders(
                httpMethod.name(), 
                region, 
                service, 
                operation, 
                uri, 
                queryParams, 
                requestBody,
                credentials);
            
            // Create request entity
            HttpEntity<?> requestEntity = new HttpEntity<>(body, headers);
            
            // Make the request
            return restTemplate.exchange(url, httpMethod, requestEntity, responseType);
        } catch (Exception e) {
            log.error("Error sending authenticated request to AWS: {}", e.getMessage(), e);
            throw new RuntimeException("Error sending authenticated request to AWS", e);
        }
    }
    
    /**
     * Helper methods
     */
    private String extractUri(String url) {
        try {
            int queryStart = url.indexOf('?');
            if (queryStart == -1) {
                return url.substring(url.indexOf("/", 8)); // 8 to skip "https://"
            } else {
                return url.substring(url.indexOf("/", 8), queryStart);
            }
        } catch (Exception e) {
            return "/";
        }
    }
    
    private String extractQueryParams(String url) {
        int queryStart = url.indexOf('?');
        if (queryStart == -1) {
            return "";
        } else {
            return url.substring(queryStart + 1);
        }
    }
    
    private String convertBodyToString(Object body) {
        if (body == null) {
            return "";
        } else if (body instanceof String) {
            return (String) body;
        } else {
            try {
                return new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(body);
            } catch (Exception e) {
                return body.toString();
            }
        }
    }
}