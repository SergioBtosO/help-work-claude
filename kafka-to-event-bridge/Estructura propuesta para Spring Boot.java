src/main/java/com/tuempresa/
├── config/
│   ├── KafkaConfig.java         // Configuración de Kafka
│   ├── AwsConfig.java           // Configuración de AWS/EventBridge
│   └── RedisConfig.java         // Configuración de Redis
├── listener/
│   └── KafkaEventListener.java  // Listeners de Kafka
├── handler/
│   └── EventBridgeHandler.java  // Manejadores
├── orchestrator/
│   ├── EventBridgeOrchestrator.java
│   └── KafkaMessageOrchestrator.java
├── model/
│   └── PaymentEvent.java        // Modelos de datos
├── exception/
│   └── GlobalExceptionHandler.java
└── health/
    └── HealthCheckController.java