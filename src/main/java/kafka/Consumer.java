package kafka;

import com.opencsv.bean.CsvToBeanBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        ArrayList<String> listId = new ArrayList<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "app-readcsv");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singleton(AppConfigs.topicName));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        String file = "data/output.csv";
                        List<spotify> beans1 = new CsvToBeanBuilder(new FileReader(file)).withType(spotify.class).build().parse();
                        for (spotify c : beans1) {
                            listId.add(c.getTitle());
                        }


                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    List<spotify> beans = new CsvToBeanBuilder(new StringReader(record.value())).withType(spotify.class).build().parse();
                    for (spotify c : beans) {
                        System.out.println(c.getId() + "," + c.getNumOrder() + "," + c.getAge() + "," + c.getTel());


                        if (!listId.contains(c.getId()) && Integer.parseInt(c.getAge()) < 20 && Integer.parseInt(c.getNumOrder()) > 100) {
                            File file1 = new File("data/output.csv");
                            try {
                                FileWriter fileWriter = new FileWriter(file1, true);

                                fileWriter.append(c.getId() + ",");
                                fileWriter.append(c.getNumOrder() + ",");
                                fileWriter.append(c.getAge() + ",");
                                fileWriter.append(c.getTel());
                                fileWriter.append("\n");

                                fileWriter.flush();
                                fileWriter.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }

                    }
                }
            }
        } finally {
            consumer.close();
        }
    }
}

