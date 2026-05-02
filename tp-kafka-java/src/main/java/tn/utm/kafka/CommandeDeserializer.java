package tn.utm.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import java.util.Map;

public class CommandeDeserializer implements Deserializer<Commande> {
    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public Commande deserialize(String topic, byte[] data) {
        try {
            if (data == null) return null;
            return objectMapper.readValue(data, Commande.class);
        } catch (Exception e) {
            throw new RuntimeException("Erreur de désérialisation JSON", e);
        }
    }

    @Override
    public void close() {}
}
