package com.example.kafka.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class MessageService {
    private static final Logger logger = LoggerFactory.getLogger(MessageService.class);

    /**
     * Procesa un mensaje recibido
     * @param message El mensaje a procesar
     * @return true si el procesamiento fue exitoso, false en caso contrario
     */
    public boolean processMessage(String message) {
        try {
            logger.info("Procesando mensaje: {}", message);
            // Aquí iría la lógica de negocio para procesar el mensaje
            // Por ejemplo, validar, transformar, almacenar en base de datos, etc.
            
            return true;
        } catch (Exception e) {
            logger.error("Error al procesar el mensaje: {}", message, e);
            return false;
        }
    }
}
