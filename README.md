# Projet Apache Kafka avec Java

Ce dépôt contient deux projets illustrant l'utilisation d'Apache Kafka avec Java :
1. **tp-kafka-java** : Concepts de base (Producer/Consumer personnalisés avec Sérialisation JSON).
2. **mini-projet** : Simulation de flux de données (Events POS) avec détection d'anomalies et agrégation en temps réel.

## Prérequis

- **Java 17** ou supérieur.
- **Apache Maven**.
- **Apache Kafka** installé localement (ex: dans `C:\kafka`).

---

## 1. Démarrage de l'infrastructure Kafka

Ouvrez des terminaux dans votre répertoire d'installation Kafka (`C:\kafka`) :



### B. Lancer le serveur Kafka
```powershell
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

### C. Créer les Topics requis
```powershell
# Pour le TP de base
.\bin\windows\kafka-topics.bat --create --topic ventes-json --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# Pour le Mini-Projet
.\bin\windows\kafka-topics.bat --create --topic pos-events --bootstrap-server localhost:9092 --partitions 4 --replication-factor 1
```

---

## 2. Utilisation du TP (tp-kafka-java)

Ce projet montre comment envoyer et recevoir des objets `Commande` sérialisés en JSON.

1. **Compiler le projet** :
   ```powershell
   cd tp-kafka-java
   mvn clean compile
   ```

2. **Lancer le Consommateur** (dans un terminal) :
   ```powershell
   mvn exec:java "-Dexec.mainClass=tn.utm.kafka.SimpleConsumer"
   ```

3. **Lancer le Producteur** (dans un autre terminal) :
   ```powershell
   mvn exec:java "-Dexec.mainClass=tn.utm.kafka.SimpleProducer"
   ```

---

## 3. Utilisation du Mini-Projet (mini-projet)

Simulation d'un système de point de vente (POS) avec traitement analytique.

1. **Compiler le projet** :
   ```powershell
   cd mini-projet
   mvn clean compile
   ```

2. **Lancer les composants** (recommandé dans des terminaux séparés) :
   
   - **Simulateur de Caisses** (Générateur de données) :
     ```powershell
     mvn exec:java "-Dexec.mainClass=tn.utm.kafka.SimulateurCaisse"
     ```
   
   - **Détecteur d'Anomalies** (Filtre les retours suspects > 200 DT) :
     ```powershell
     mvn exec:java "-Dexec.mainClass=tn.utm.kafka.DetecteurAnomalie"
     ```
   
   - **Calculateur de Chiffre d'Affaires** (Agrégation par ville) :
     ```powershell
     mvn exec:java "-Dexec.mainClass=tn.utm.kafka.ChiffreAffairesParVille"
     ```

---

## Outils de débogage utiles

- **Lister les topics** :
  ```powershell
  .\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092
  ```

- **Consommer les messages brute (CLI)** :
  ```powershell
  .\bin\windows\kafka-console-consumer.bat --topic pos-events --from-beginning --bootstrap-server localhost:9092
  ```
