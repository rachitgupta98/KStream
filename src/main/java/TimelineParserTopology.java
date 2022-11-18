import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.JSONPObject;
import com.google.gson.JsonObject;
import lombok.var;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

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
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        return properties;
    }

    public static Topology createTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), new JsonSerde<JsonObject>(JsonObject.class)))
                .filter((key,value) -> {
                    return value.has("response") && value.get("response").getAsJsonObject().has("id");
                }).map((key,value) -> KeyValue.pair(value.get("response").getAsJsonObject().get("id"), value.get("response").getAsJsonObject().get("adm"))).peek((k, v) ->
        {


            System.out.println(v);


        }).to(OUTPUT_TOPIC);
        return streamsBuilder.build();
    }
}
