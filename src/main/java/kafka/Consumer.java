//package kafka;
//
//import com.opencsv.bean.CsvToBeanBuilder;
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//import org.apache.kafka.common.serialization.StringDeserializer;
//
//import java.io.*;
//import java.text.ParseException;
//import java.text.SimpleDateFormat;
//import java.util.*;
//
//public class Consumer {
//    public static void main(String[] args) throws ParseException {
//        Properties props = new Properties();
//        ArrayList<String> listSong = new ArrayList<>();
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
//        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        props.put(ConsumerConfig.GROUP_ID_CONFIG, "app-readcsv");
//
//        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
//        consumer.subscribe(Collections.singleton(AppConfigs.topicName));
//        String sDate1 = "2017-01-01";
//        Date date1 = new SimpleDateFormat("yyyy-MM-dd").parse(sDate1);
//        try {
//            while (true) {
//                ConsumerRecords<String, String> records = consumer.poll(100);
//                for (ConsumerRecord<String, String> record : records) {
//                    try {
//                        String file = "data/2017-01-01.csv";
//                        List<spotify> beans1 = new CsvToBeanBuilder(new FileReader(file)).withType(spotify.class).build().parse();
//                        for (spotify c : beans1) {
//                            listSong.add(c.getTitle());
//                        }
//
//
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
//                    List<spotify> beans = new CsvToBeanBuilder(new StringReader(record.value())).withType(spotify.class).build().parse();
//                    for (spotify c : beans) {
//                        System.out.println(c.getTitle()+ c.getRank()+" "+c.getArtist()+" "+c.getStreams()+" "+c.getTrend()+" "+c.getRegion()+" "+c.getChart());
//                          if (listSong.contains(c.getDate() == date1 )) {
//                            File file1 = new File("data/2017-01-01.csv");
//                            try {
//                                FileWriter fileWriter = new FileWriter(file1, true);
//                                fileWriter.append(c.getTitle() + ",");
//                                fileWriter.append(c.getRank() + ",");
//                                fileWriter.append(c.getDate() + ",");
//                                fileWriter.append(c.getArtist() + ",");
//                                fileWriter.append(c.getRegion() + ",");
//                                fileWriter.append(c.getChart() + ",");
//                                fileWriter.append(c.getTrend() + ",");
//                                fileWriter.append(c.getStreams());
//                                fileWriter.append("\n");
//
//                                fileWriter.flush();
//                                fileWriter.close();
//                            } catch (IOException e) {
//                                e.printStackTrace();
//                            }
//                        }
//
//                    }
//                }
//            }
//        } finally {
//            consumer.close();
//        }
//    }
//}
//
