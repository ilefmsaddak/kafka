package tn.utm.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.*;

public class ChiffreAffairesParVille {
    private static final Map<String, Double> caCumule = new HashMap<>();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ca-1");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JSONSerde.class.getName());
        props.put("json.target.class", PosEvent.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        try (Consumer<String, PosEvent> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("pos-events"));
            
            long lastDisplay = System.currentTimeMillis();
            while (true) {
                ConsumerRecords<String, PosEvent> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, PosEvent> record : records) {
                    PosEvent event = record.value();
                    if (event == null) continue;

                    double currentCA = caCumule.getOrDefault(event.getVille(), 0.0);
                    if ("VENTE".equals(event.getType())) {
                        caCumule.put(event.getVille(), currentCA + event.getMontant());
                        System.out.println("✅ Traité : VENTE à " + event.getVille() + " (" + String.format("%.2f", event.getMontant()) + " DT)");
                    } else if ("RETOUR".equals(event.getType())) {
                        caCumule.put(event.getVille(), currentCA - event.getMontant());
                        System.out.println("🔄 Traité : RETOUR à " + event.getVille() + " (" + String.format("%.2f", event.getMontant()) + " DT)");
                    }
                }

                if (!records.isEmpty()) {
                    consumer.commitSync();
                }

                if (System.currentTimeMillis() - lastDisplay > 5000) {
                    afficherRapport();
                    lastDisplay = System.currentTimeMillis();
                }
            }
        }
    }

    private static void afficherRapport() {
        System.out.println("\n📊 --- RAPPORT CHIFFRE D'AFFAIRES PAR VILLE ---");
        caCumule.forEach((ville, total) -> System.out.printf("%-10s : %.2f DT%n", ville, total));
        System.out.println("------------------------------------------------");
    }
}
