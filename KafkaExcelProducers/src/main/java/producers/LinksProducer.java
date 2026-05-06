package producers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.fasterxml.jackson.databind.ObjectMapper;

public class LinksProducer {

    public static void main(String[] args) throws Exception {

        String csvPath = "D:\\PROJECTS\\BIG_DATA_PROJECT\\Datasets\\links.csv";
        String topic = "links_raw";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ObjectMapper mapper = new ObjectMapper();

        BufferedReader br = new BufferedReader(new FileReader(csvPath));
        String line;
        boolean header = true;

        while ((line = br.readLine()) != null) {
            if (header) { header = false; continue; }

            String[] c = line.split(",");

            String movieId = c[0];
            String imdbId = c[1];
            String tmdbId = c.length >= 3 ? c[2] : "";

            Map<String, Object> json = new HashMap<>();
            json.put("movieId", Integer.parseInt(movieId));
            json.put("imdbId", imdbId);
            json.put("tmdbId", tmdbId);

            String jsonStr = mapper.writeValueAsString(json);

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, movieId, jsonStr);

            RecordMetadata meta = producer.send(record).get();

            System.out.println("Sent Link movieId=" + movieId +
                    " Partition=" + meta.partition() + " Offset=" + meta.offset());

            Thread.sleep(20);
        }

        br.close();
        producer.close();

        System.out.println("Links CSV sent successfully!");
    }
}
