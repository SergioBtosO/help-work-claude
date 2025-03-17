package com.example.kafka.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class MessageService {
    private static final Logger logger = LoggerFactory.getLogger(MessageService.class);

    private final AvroService avroService;
    
    @Autowired
    public MessageService(AvroService avroService) {
        this.avroService = avroService;
    }

    /**
     * Procesa un mensaje recibido
     * @param message El mensaje a procesar
     * @return true si el procesamiento fue exitoso, false en caso contrario
     */
    public boolean processMessage(String message) {
        try {
            logger.info("Procesando mensaje: {}", message);
            
            // Crear una copia del mensaje original
            String messageCopy = new String(message);
            
            // Realizar el procesamiento del mensaje (lógica de negocio)
            // ...
            
            // Enviar la copia al servicio AVRO para convertirla y reemplazarla
            boolean avroResult = avroService.processAndReplaceMessage(messageCopy);
            
            if (avroResult) {
                logger.info("Mensaje procesado y convertido a AVRO exitosamente");
            } else {
                logger.warn("El mensaje se procesó pero hubo problemas con la conversión a AVRO");
            }
            
            return true;
        } catch (Exception e) {
            logger.error("Error al procesar el mensaje: {}", message, e);
            return false;
        }
    }
}
