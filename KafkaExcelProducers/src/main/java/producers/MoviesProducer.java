package producers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.databind.ObjectMapper;

public class MoviesProducer {

    public static void main(String[] args) {

        String csvPath = "D:\\PROJECTS\\BIG_DATA_PROJECT\\Datasets\\output_20000_rows.csv";
        String topic = "movies";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");
        props.put("retries", "3");
        props.put("linger.ms", "5");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        ObjectMapper mapper = new ObjectMapper();

        try (BufferedReader br = new BufferedReader(new FileReader(csvPath))) {

            String line;
            boolean header = true;
            int count = 0;          // إجمالي عدد السجلات المرسلة
            int batchCount = 0;     // عداد الـ Batch (1000)

            while ((line = br.readLine()) != null) {

                if (header) {
                    header = false;
                    continue;
                }

                String[] cols = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

                if (cols.length < 8) continue;

                try {

                    String userId = cols[0].trim();
                    String movieId = cols[1].trim();
                    String rating = cols[2].trim();
                    String ratingTime = cols[3].trim();
                    String title = cols[4].trim().replaceAll("^\"|\"$", "");
                    String genres = cols[5].trim().replaceAll("^\"|\"$", "");
                    String imdbId = cols[6].trim();
                    String tmdbId = cols[7].trim();

                    if (userId.isEmpty() || movieId.isEmpty() || rating.isEmpty() || ratingTime.isEmpty()) {
                        continue;
                    }

                    Map<String, Object> jsonMap = new HashMap<>();
                    jsonMap.put("userId", Integer.parseInt(userId));
                    jsonMap.put("movieId", Integer.parseInt(movieId));
                    jsonMap.put("rating", Double.parseDouble(rating));
                    jsonMap.put("rating_time", Long.parseLong(ratingTime));
                    jsonMap.put("title", title);
                    jsonMap.put("genres", genres);
                    jsonMap.put("imdbId", imdbId);
                    jsonMap.put("tmdbId", tmdbId);

                    String json = mapper.writeValueAsString(jsonMap);

                    producer.send(new ProducerRecord<>(topic, movieId, json));

                    count++;
                    batchCount++;

                    // طباعة كل 1000
                    if (count % 1000 == 0) {
                        System.out.println("✅ Sent " + count + " records...");
                    }

                    // لو خلّصنا Batch فيها 1000 → نوقف 10 ثواني
                    if (batchCount == 1000) {
                        System.out.println("⏳ Sleeping 10 seconds before sending next batch...");
                        Thread.sleep(10_000);
                        batchCount = 0; // إعادة العداد
                    }

                } catch (Exception e) {
                    System.out.println("⚠️ Skipped bad line");
                }
            }

            producer.flush();
            System.out.println("🎯 Finished sending " + count + " records to topic: " + topic);

        } catch (Exception e) {
            System.out.println("❌ File error: " + e.getMessage());
        } finally {
            producer.close();
        }
    }
}
