package com.github.sroigmas.avro;

import com.github.sroigmas.avro.schema.Customer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.Objects;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaAvroProducerV1 {

  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
    properties.setProperty("acks", "1");
    properties.setProperty("retries", "10");

    properties.setProperty("key.serializer", StringSerializer.class.getName());
    properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
    properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");

    String topic = "customer-avro";
    Customer customer = Customer.newBuilder()
        .setFirstName("John")
        .setLastName("Doe")
        .setAge(26)
        .setHeight(185.5f)
        .setWeight(85.6f)
        .setAutomatedEmail(false)
        .build();

    ProducerRecord<String, Customer> producerRecord = new ProducerRecord<>(topic,
        customer);

    try (KafkaProducer<String, Customer> kafkaProducer = new KafkaProducer<>(properties)) {
      kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
        if (Objects.isNull(e)) {
          System.out.println(recordMetadata);
        } else {
          e.printStackTrace();
        }
      });
    }
  }
}
