package tn.utm.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.time.LocalDateTime;
import java.util.*;

public class SimulateurCaisse {
    private static final List<String> VILLES = Arrays.asList("Tunis", "Sousse", "Sfax", "Bizerte", "Gabès");
    private static final Random RANDOM = new Random();

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JSONSerde.class.getName());
        props.put("json.target.class", PosEvent.class.getName());

        // Config Sécurité/Fiabilité
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        try (Producer<String, PosEvent> producer = new KafkaProducer<>(props)) {
            System.out.println("🚀 Simulateur de Caisses démarré...");
            while (true) {
                String ville = VILLES.get(RANDOM.nextInt(VILLES.size()));
                String type = genererType();
                double montant = (type.equals("OUVERTURE")) ? 0 : 5 + (RANDOM.nextDouble() * 495);
                
                PosEvent event = new PosEvent(
                        type,
                        "CAISSE-" + ville.toUpperCase() + "-" + RANDOM.nextInt(10),
                        ville,
                        LocalDateTime.now(),
                        montant,
                        Arrays.asList("Produit-A", "Produit-B")
                );

                producer.send(new ProducerRecord<>("pos-events", ville, event));
                
                // AJOUT : Affichage explicite pour les captures d'écran
                System.out.printf("📤 Envoi : %s | %.2f DT (%s)%n", ville, montant, type);
                
                Thread.sleep(100 + RANDOM.nextInt(400));
            }
        }
    }

    private static String genererType() {
        int p = RANDOM.nextInt(100);
        if (p < 70) return "VENTE";
        if (p < 80) return "RETOUR";
        return "OUVERTURE";
    }
}
