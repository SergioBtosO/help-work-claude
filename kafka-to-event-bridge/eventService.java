package com.empresa.connector.service;

/**
 * Service interface for AWS EventBridge operations
 */
public interface EventBridgeService {
    
    /**
     * Sends a message to EventBridge with AWS1 configuration
     * 
     * @param message The message to send
     * @return Result status ("OK" or "KO")
     */
    String sendToEventBridgeWithAws1Config(String message);
    
    /**
     * Sends a message to EventBridge with AWS2 configuration
     * 
     * @param message The message to send
     * @return Result status ("OK" or "KO")
     */
    String sendToEventBridgeWithAws2Config(String message);
    
    /**
     * Sends a message to EventBridge based on the target in the message
     * 
     * @param message The message to send
     */
    void sendToEventBridge(String message);
    
    /**
     * Sends an acknowledgment message
     */
    void sendAckMessage();
}

package com.empresa.connector.service.impl;

import com.empresa.connector.config.properties.EventBridgeProperties;
import com.empresa.connector.model.AwsCredentials;
import com.empresa.connector.model.EventBridgeResponse;
import com.empresa.connector.model.eventbridge.EventBridgeMessage;
import com.empresa.connector.service.AwsAuthService;
import com.empresa.connector.service.AwsIamService;
import com.empresa.connector.service.EventBridgeService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.awssdk.services.eventbridge.model.PutEventsResponse;

import javax.annotation.Resource;

@Slf4j
@Service
@RequiredArgsConstructor
public class EventBridgeServiceImpl implements EventBridgeService {

    private final EventBridgeProperties eventBridgeProperties;
    private final AwsIamService awsIamService;
    private final AwsAuthService awsAuthService;
    private final ObjectMapper objectMapper;
    
    @Value("${topics.output.source:openbank.payments}")
    private String eventSource;
    
    @Value("${topics.output.detail-type:Transfer_KO}")
    private String detailType;
    
    @Value("${aws.eventbridge.aws1.event-bus-name}")
    private String eventBusNameAws1;
    
    @Value("${aws.eventbridge.aws2.event-bus-name}")
    private String eventBusNameAws2;
    
    @Resource(name = "eventBridgeClientAws1")
    private EventBridgeClient eventBridgeClientAws1;
    
    @Resource(name = "eventBridgeClientAws2")
    private EventBridgeClient eventBridgeClientAws2;
    
    @Override
    public String sendToEventBridgeWithAws1Config(String message) {
        try {
            log.info("Sending to AWS1 EventBridge");
            
            // Get credentials
            AwsCredentials credentials = awsIamService.getAwsCredentialsForAws1();
            
            // Get AWS1 EventBridge properties
            EventBridgeProperties.EventBridgeInstanceProperties aws1Props = eventBridgeProperties.getAws1();
            
            // Parse message to check if it's valid
            EventBridgeMessage eventBridgeMessage = parseMessage(message);
            
            // Construct URL
            String url = String.format("https://%s", aws1Props.getHost());
            
            // Send authenticated request
            ResponseEntity<EventBridgeResponse> response = awsAuthService.sendAuthenticatedRequest(
                url,
                HttpMethod.POST,
                eventBridgeMessage,
                EventBridgeResponse.class,
                credentials,
                aws1Props.getRegion(),
                eventBridgeProperties.getService(),
                eventBridgeProperties.getAmzTarget()
            );
            
            // Check response
            if (response.getStatusCode().is2xxSuccessful() && 
                response.getBody() != null && 
                response.getBody().getFailedEntryCount() == 0) {
                log.info("Events successfully sent to AWS1 EventBridge");
                return eventBridgeProperties.getResult().getCorrect();
            } else {
                log.error("Error sending events to AWS1 EventBridge: {}", response.getBody());
                return eventBridgeProperties.getResult().getIncorrect();
            }
            
        } catch (Exception e) {
            log.error("Error sending to AWS1 EventBridge: {}", e.getMessage(), e);
            return eventBridgeProperties.getResult().getIncorrect();
        }
    }
    
    @Override
    public String sendToEventBridgeWithAws2Config(String message) {
        try {
            log.info("Sending to AWS2 EventBridge");
            
            // Get credentials
            AwsCredentials credentials = awsIamService.getAwsCredentialsForAws2();
            
            // Get AWS2 EventBridge properties
            EventBridgeProperties.EventBridgeInstanceProperties aws2Props = eventBridgeProperties.getAws2();
            
            // Parse message to check if it's valid
            EventBridgeMessage eventBridgeMessage = parseMessage(message);
            
            // Construct URL
            String url = String.format("https://%s", aws2Props.getHost());
            
            // Send authenticated request
            ResponseEntity<EventBridgeResponse> response = awsAuthService.sendAuthenticatedRequest(
                url,
                HttpMethod.POST,
                eventBridgeMessage,
                EventBridgeResponse.class,
                credentials,
                aws2Props.getRegion(),
                eventBridgeProperties.getService(),
                eventBridgeProperties.getAmzTarget()
            );
            
            // Check response
            if (response.getStatusCode().is2xxSuccessful() && 
                response.getBody() != null && 
                response.getBody().getFailedEntryCount() == 0) {
                log.info("Events successfully sent to AWS2 EventBridge");
                return eventBridgeProperties.getResult().getCorrect();
            } else {
                log.error("Error sending events to AWS2 EventBridge: {}", response.getBody());
                return eventBridgeProperties.getResult().getIncorrect();
            }
            
        } catch (Exception e) {
            log.error("Error sending to AWS2 EventBridge: {}", e.getMessage(), e);
            return eventBridgeProperties.getResult().getIncorrect();
        }
    }
    
    @Override
    public void sendToEventBridge(String message) {
        try {
            // Determine target AWS
            if (message.contains("\"awsDestiny\":\"aws2\"")) {
                sendToEventBridgeWithAws2Config(message);
            } else {
                sendToEventBridgeWithAws1Config(message);
            }
        } catch (Exception e) {
            log.error("Error sending to EventBridge: {}", e.getMessage(), e);
            throw new RuntimeException("Error sending to EventBridge", e);
        }
    }
    
    @Override
    public void sendAckMessage() {
        log.info("Sending ACK message");
        // Implementation for sending acknowledgment message
        // This would depend on the specific requirements
    }
    
    /**
     * Parse message to EventBridgeMessage
     */
    private EventBridgeMessage parseMessage(String message) {
        try {
            if (message instanceof String) {
                return objectMapper.readValue(message, EventBridgeMessage.class);
            } else {
                return (EventBridgeMessage) message;
            }
        } catch (Exception e) {
            log.warn("Error parsing message to EventBridgeMessage: {}", e.getMessage());
            
            // Create default EventBridge message
            return EventBridgeMessage.builder()
                .detailType(detailType)
                .source(eventSource)
                .detail(message)
                .build();
        }
    }
}