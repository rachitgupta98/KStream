import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.var;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;

import java.sql.Time;
import java.util.Properties;

public class TimelineParserTopology {
    private static final String INPUT_TOPIC = "sentences";
    private static final String OUTPUT_TOPIC = "word-count";
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static Properties getConfig() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        return properties;
    }

    public static Topology createTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), new JsonSerde<>(TimelineModel.class))).peek((k, v) ->
        {


                System.out.println(v);
                System.out.println(v.response);

        }).to(OUTPUT_TOPIC);
        return streamsBuilder.build();
    }
}
