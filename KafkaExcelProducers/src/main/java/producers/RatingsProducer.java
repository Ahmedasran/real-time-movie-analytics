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

public class RatingsProducer {

    public static void main(String[] args) throws Exception {

        String csvPath = "D:\\PROJECTS\\BIG_DATA_PROJECT\\Datasets\\ratings.csv";
        String topic = "ratings_raw";

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

            String[] cols = line.split(",");

            String userId = cols[0];
            String movieId = cols[1];
            String rating = cols[2];
            String ts = cols[3];

            Map<String, Object> json = new HashMap<>();
            json.put("userId", Integer.parseInt(userId));
            json.put("movieId", Integer.parseInt(movieId));
            json.put("rating", Double.parseDouble(rating));
            json.put("timestamp", Long.parseLong(ts));

            String jsonStr = mapper.writeValueAsString(json);

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, movieId, jsonStr);

            RecordMetadata meta = producer.send(record).get();

            System.out.println("Sent Rating: movieId=" + movieId +
                    " Partition=" + meta.partition() + " Offset=" + meta.offset());

            Thread.sleep(30);
        }

        br.close();
        producer.close();

        System.out.println("Ratings CSV sent successfully!");
    }
}
