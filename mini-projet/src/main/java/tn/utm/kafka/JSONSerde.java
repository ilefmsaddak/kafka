package tn.utm.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;
import java.util.Map;

public class JSONSerde<T> implements Serializer<T>, Deserializer<T> {
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private Class<T> targetClass;

    public JSONSerde() {}
    public JSONSerde(Class<T> targetClass) { this.targetClass = targetClass; }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (targetClass == null) {
            String className = (String) configs.get("json.target.class");
            try { targetClass = (Class<T>) Class.forName(className); } 
            catch (Exception e) { throw new RuntimeException(e); }
        }
    }

    @Override
    public byte[] serialize(String topic, T data) {
        try { return data == null ? null : objectMapper.writeValueAsBytes(data); }
        catch (Exception e) { throw new RuntimeException(e); }
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        try { return data == null ? null : objectMapper.readValue(data, targetClass); }
        catch (Exception e) { throw new RuntimeException(e); }
    }

    @Override
    public void close() {
        // No resources to close
    }
}
