import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaClient {

    public static void main(String[] args) {
        String topic = "test-topic";
        String groupId = "simple-kafka-app-group";
        String testString = "{\"radioId\":\"id-0\",\"subscriptionEventType\":\"activate\",\"createdTimestamp\":\"2021-11-04T15:24:17.247\",\"publishedTimestamp\":\"2021-11-04T15:24:17.251\",\"transactionId\":\"tr-0\",\"vehicleMake\":\"Ford\",\"vehicleModel\":\"Retro\",\"modelYear\":\"2010\",\"color\":\"blue\"}";
        Properties properties = new Properties();
        Map<String, Object> producerconfig = new HashMap<>(); //producer config
        Map<String, Object> consumerConfig = new HashMap<>(); //consumer Config
        //producer config
        producerconfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerconfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerconfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerconfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        //consumer config
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // create producer and send message
        Producer<String, String> producer = new KafkaProducer<>(producerconfig);
        // create consumer, subscribe and poll
        Consumer<String, String> consumer = new KafkaConsumer<>(consumerConfig);
        consumer.subscribe(Arrays.asList(topic));
        System.out.println("Producing events to " + "localhost:9092" + " & topic: " + topic);
        try {
            for (int i = 0; i < 10; i++) {
                producer.send(new ProducerRecord<>(topic, ("Message: " + String.valueOf(i) + ":" + testString)));
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord record :
                        records) {
                    System.out.println("Consumer record: " + record.value());
                    consumer.commitAsync();
                }

            }

        } finally {
            producer.close();
            consumer.close();
        }
    }
}
