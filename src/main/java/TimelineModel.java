import lombok.Data;

@Data
public class TimelineModel {
    String requestId;
    Response response;

    @Data
    public static class Response {
        String id;
        String size;
        String adm;
    }
}
