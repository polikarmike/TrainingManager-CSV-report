package edu.epam;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.dynamodb.*;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.s3.*;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

public class Handler {

    private final String tableName = System.getenv("TABLE_NAME");
    private final String s3Bucket = System.getenv("S3_BUCKET");
    private final String s3Prefix = Optional.ofNullable(System.getenv("S3_PREFIX")).orElse("");

    private final DynamoDbClient dynamo = DynamoDbClient.create();
    private final S3Client s3 = S3Client.create();

    public Map<String, Object> handleRequest(Map<String, Object> event) {
        LocalDateTime startTime = LocalDateTime.now();
        System.out.println("[INFO] Lambda execution started at: " + startTime);

        Map<String, Object> result = new HashMap<>();

        try {
            if (s3Bucket == null || s3Bucket.isEmpty()) {
                throw new RuntimeException("S3_BUCKET environment variable is not set");
            }

            LocalDate now = LocalDate.now();
            int year = now.getYear();
            int month = now.getMonthValue();

            List<String[]> rows = new ArrayList<>();
            rows.add(new String[]{
                    "Trainer First Name",
                    "Trainer Last Name",
                    "Current Month Trainings Duration"
            });

            List<Map<String, AttributeValue>> items = scanAllItems(tableName);
            System.out.println("[INFO] Trainers scanned: " + items.size());

            for (Map<String, AttributeValue> item : items) {
                String status = item.getOrDefault("traineeStatus", AttributeValue.fromS("")).s();
                String first = item.getOrDefault("firstName", AttributeValue.fromS("")).s();
                String last = item.getOrDefault("lastName", AttributeValue.fromS("")).s();

                int duration = extractDuration(item, year, month);

                if ("Inactive".equalsIgnoreCase(status) && duration == 0) {
                    continue;
                }

                rows.add(new String[]{first, last, String.valueOf(duration)});
            }

            System.out.println("[INFO] Rows to write: " + (rows.size() - 1));

            StringBuilder sb = new StringBuilder();
            for (String[] line : rows) {
                sb.append(String.join(",", line)).append("\n");
            }
            String csv = sb.toString();

            String filename = String.format("Trainers_Trainings_summary_%04d_%02d.csv", year, month);
            String key = s3Prefix.isEmpty() ? filename : (s3Prefix + "/" + filename);

            PutObjectRequest req = PutObjectRequest.builder()
                    .bucket(s3Bucket)
                    .key(key)
                    .contentType("text/csv")
                    .build();

            s3.putObject(req, RequestBody.fromBytes(csv.getBytes(StandardCharsets.UTF_8)));

            System.out.println("[INFO] CSV uploaded to s3://" + s3Bucket + "/" + key);

            result.put("statusCode", 200);
            result.put("message", "Uploaded to s3://" + s3Bucket + "/" + key);
            result.put("rows_written", rows.size() - 1);

        } catch (Exception e) {
            System.err.println("[ERROR] Lambda execution failed: " + e.getMessage());
            result.put("statusCode", 500);
            result.put("message", "Lambda execution failed: " + e.getMessage());
        }

        System.out.println("[INFO] Lambda execution finished at: " + LocalDateTime.now());
        return result;
    }

    private List<Map<String, AttributeValue>> scanAllItems(String tableName) {
        List<Map<String, AttributeValue>> allItems = new ArrayList<>();
        String lastEvaluatedKey = null;

        do {
            ScanRequest.Builder scanBuilder = ScanRequest.builder().tableName(tableName);
            if (lastEvaluatedKey != null) {
                scanBuilder.exclusiveStartKey(Collections.singletonMap("trainerUsername",
                        AttributeValue.builder().s(lastEvaluatedKey).build()));
            }

            ScanResponse response = dynamo.scan(scanBuilder.build());
            allItems.addAll(response.items());

            Map<String, AttributeValue> lek = response.lastEvaluatedKey();
            lastEvaluatedKey = (lek != null && lek.containsKey("trainerUsername")) ? lek.get("trainerUsername").s() : null;
        } while (lastEvaluatedKey != null);

        return allItems;
    }

    private int extractDuration(Map<String, AttributeValue> item, int targetYear, int targetMonth) {
        if (!item.containsKey("years")) return 0;

        List<AttributeValue> yearsList = item.get("years").l();

        for (AttributeValue yearItem : yearsList) {
            Map<String, AttributeValue> yMap = yearItem.m();
            int year = Integer.parseInt(yMap.get("year").n());
            if (year != targetYear) continue;

            List<AttributeValue> months = yMap.get("months").l();
            for (AttributeValue mItem : months) {
                Map<String, AttributeValue> mMap = mItem.m();
                int month = Integer.parseInt(mMap.get("month").n());
                if (month != targetMonth) continue;

                return Integer.parseInt(mMap.get("duration").n());
            }
        }
        return 0;
    }
}
