package tn.utm.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.*;

public class DetecteurAnomalie {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "alerte-1");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JSONSerde.class.getName());
        props.put("json.target.class", PosEvent.class.getName());

        try (Consumer<String, PosEvent> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("pos-events"));
            System.out.println("🚨 Détecteur d'anomalies en veille (Groupe: alerte-1)...");

            while (true) {
                ConsumerRecords<String, PosEvent> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, PosEvent> record : records) {
                    PosEvent event = record.value();
                    if ("RETOUR".equals(event.getType()) && event.getMontant() > 200.0) {
                        System.err.printf("⚠️ ALERTE : Retour suspect à %s (Caisse %s) : %.2f DT !%n",
                                event.getVille(), event.getIdCaisse(), event.getMontant());
                    }
                }
            }
        }
    }
}
