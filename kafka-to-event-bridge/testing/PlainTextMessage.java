package com.example.kafka.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PlainTextMessage {
    /**
     * Identificador único del mensaje
     */
    private String id;

    /**
     * Contenido del mensaje
     */
    private String content;

    /**
     * Marca de tiempo del mensaje
     */
    private Long timestamp;

    /**
     * Origen del mensaje
     */
    private String source;

    /**
     * Propiedades adicionales del mensaje
     */
    @Builder.Default
    private Map<String, String> properties = new HashMap<>();

    /**
     * Constructor de copia defensiva para propiedades
     * @return Una copia del mapa de propiedades
     */
    public Map<String, String> getProperties() {
        return properties != null ? new HashMap<>(properties) : new HashMap<>();
    }

    /**
     * Método para agregar una propiedad
     * @param key Clave de la propiedad
     * @param value Valor de la propiedad
     * @return El mensaje actual para encadenamiento
     */
    public PlainTextMessage addProperty(String key, String value) {
        if (this.properties == null) {
            this.properties = new HashMap<>();
        }
        this.properties.put(key, value);
        return this;
    }

    /**
     * Método de fábrica para crear un mensaje simple
     * @param content Contenido del mensaje
     * @return Una nueva instancia de PlainTextMessage
     */
    public static PlainTextMessage of(String content) {
        return PlainTextMessage.builder()
                .id(null)
                .content(content)
                .timestamp(System.currentTimeMillis())
                .source(null)
                .build();
    }
}
