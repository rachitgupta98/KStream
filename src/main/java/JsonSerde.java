import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

public class JsonSerde<T> implements Serde<T> {
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final Class<T> type;
    private final Gson gson = new GsonBuilder().create();

    public JsonSerde(Class<T> type) {
        this.type = type;
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, data) -> serialize(data);
    }

    @SneakyThrows
    private byte[] serialize(T data) {
        if (data == null)
            return null;

        return gson.toJson(data).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, bytes) -> deserialize(bytes);
    }

    @SneakyThrows
    private T deserialize(byte[] bytes) {
        return gson.fromJson(new String(bytes, StandardCharsets.UTF_8), type);
    }
}
