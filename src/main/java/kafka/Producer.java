//package kafka;
//
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerConfig;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.common.serialization.StringSerializer;
//
//import java.io.File;
//import java.io.FileNotFoundException;
//import java.util.List;
//import java.util.Properties;
//import java.util.Scanner;
//
//public class Producer {
//
//    public static void main(String[] args) throws Exception {
//
//        Properties props = new Properties();
//        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
//        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//
//        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
//
//        try {
//            File obj = new File("E:\\charts.csv");
//            Scanner reader = new Scanner(obj);
//            while (reader.hasNextLine()) {
//                String data = reader.nextLine();
//                producer.send(new ProducerRecord<>(AppConfigs.topicName, null, data));
//                Thread.sleep(1000);
//                System.out.println(data);
//            }
//            reader.close();
//            producer.close();
//        } catch (FileNotFoundException e) {
//            System.out.println("An error occurred.");
//            e.printStackTrace();
//        }
//    }
//}
