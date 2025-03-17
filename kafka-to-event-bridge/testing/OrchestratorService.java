package com.example.kafka.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
public class OrchestratorService {
    private static final Logger logger = LoggerFactory.getLogger(OrchestratorService.class);

    /**
     * Procesa el mensaje recibido de Kafka
     * @param message Contenido del mensaje
     * @return true si el procesamiento fue exitoso
     */
    public boolean processMessage(String message) {
        try {
            // Imprimir el mensaje
            logger.info("Mensaje recibido: {}", message);
            
            // Aquí puedes agregar cualquier lógica adicional de procesamiento
            
            return true;
        } catch (Exception e) {
            logger.error("Error procesando mensaje: {}", message, e);
            return false;
        }
    }
}
