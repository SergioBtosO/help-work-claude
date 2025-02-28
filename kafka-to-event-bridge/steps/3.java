/**
 * MODELO DE KAFKA
 */
package com.empresa.connector.model.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class KafkaPaymentMessage {
    
    @JsonProperty("G6181_IDEMPR")
    private String idempr;
    
    @JsonProperty("G6181_CCENCONT")
    private String ccencont;
    
    @JsonProperty("G6181_TIPOPRD")
    private String tipoprd;
    
    @JsonProperty("G6181_CCONTRAT")
    private String ccontrat;
    
    @JsonProperty("G6181_NUMORD")
    private String numord;
    
    @JsonProperty("G6181_JNUMDET")
    private String jnumdet;
    
    @JsonProperty("G6181_FECHAEJE")
    private String fechaeje;
    
    @JsonProperty("G6181_CODESTA2")
    private String codesta2;
    
    // Método para validar el CODESTA2
    public boolean isValidCodesta2(String validValue) {
        return validValue.equals(codesta2);
    }
    
    // Método para construir el operationId según el formato requerido
    public String buildOperationId() {
        return String.format("%s-%s-%s-%s",
                ccencont + numord + jnumdet,
                "01001",
                "00000",
                fechaeje);
    }
}