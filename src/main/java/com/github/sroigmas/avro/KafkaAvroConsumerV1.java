package com.github.sroigmas.avro;

import com.github.sroigmas.avro.schema.Customer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaAvroConsumerV1 {

  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
    properties.setProperty("group.id", "my-avro-consumer");
    properties.setProperty("enable.auto.commit", "false");
    properties.setProperty("auto.offset.reset", "earliest");

    properties.setProperty("key.deserializer", StringDeserializer.class.getName());
    properties.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
    properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");
    properties.setProperty("specific.avro.reader", "true");

    String topic = "customer-avro";
    KafkaConsumer<String, Customer> consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(List.of(topic));

    System.out.println("Waiting for data...");
    while (true) {
      ConsumerRecords<String, Customer> records = consumer.poll(Duration.ofMillis(500));
      for (ConsumerRecord<String, Customer> record : records) {
        Customer customer = record.value();
        System.out.println(customer);
      }
      consumer.commitSync();
    }
  }
}
