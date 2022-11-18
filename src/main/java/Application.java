import org.apache.kafka.streams.KafkaStreams;

public class Application {
    public static void main(String[] args) {
        KafkaStreams kafkaStreams = new KafkaStreams(TimelineParserTopology.createTopology(), TimelineParserTopology.getConfig());

        // Start the application
        kafkaStreams.start();

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}
