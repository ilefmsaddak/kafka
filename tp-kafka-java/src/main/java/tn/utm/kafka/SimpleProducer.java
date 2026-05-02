package tn.utm.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Properties;

public class SimpleProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Utilisation du sérialiseur custom
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CommandeSerializer.class.getName());

        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        try (Producer<String, Commande> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i <= 5; i++) {
                Commande cmd = new Commande(
                        "CMD-" + i,
                        LocalDateTime.now(),
                        Arrays.asList("Article-" + i, "Article-Plus-" + i),
                        100.0 + (i * 25.5)
                );

                ProducerRecord<String, Commande> record = new ProducerRecord<>("ventes-json", cmd.getId(), cmd);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Échec : " + exception.getMessage());
                    } else {
                        System.out.printf("✓ Commande envoyée : %s (partition=%d)%n", cmd.getId(), metadata.partition());
                    }
                });
            }
            producer.flush();
        }
    }
}