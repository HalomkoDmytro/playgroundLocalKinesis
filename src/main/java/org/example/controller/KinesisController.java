package org.example.controller;

import org.example.service.KinesisService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/kinesis")
public class KinesisController {

    @Autowired
    private KinesisService kinesisService;

    @PostMapping("/send")
    public ResponseEntity<Map<String, String>> sendMessage(@RequestParam String partitionKey,
                                                           @RequestBody Map<String, Object> data) {

        kinesisService.sendMessage(partitionKey, data);

        Map<String, String> response = new HashMap<>();
        response.put("status", "success");
        response.put("message", "Message sent to Kinesis stream");

        return ResponseEntity.ok(response);
    }

    @PostMapping("/send-async")
    public ResponseEntity<Map<String, String>> sendMessageAsync(
            @RequestParam String partitionKey,
            @RequestBody Map<String, Object> data) {

        kinesisService.sendMessageAsync(partitionKey, data);

        Map<String, String> response = new HashMap<>();
        response.put("status", "success");
        response.put("message", "Message sent asynchronously to Kinesis stream");

        return ResponseEntity.ok(response);
    }

    @GetMapping("/read")
    public ResponseEntity<List<Map<String, Object>>> readMessages(
            @RequestParam(defaultValue = "TRIM_HORIZON") String shardIteratorType) {

        List<Record> records = kinesisService.readMessages(shardIteratorType);

        List<Map<String, Object>> response = records.stream()
                .map(record -> {
                    Map<String, Object> recordMap = new HashMap<>();
                    recordMap.put("partitionKey", record.partitionKey());
                    recordMap.put("sequenceNumber", record.sequenceNumber());
                    recordMap.put("data", record.data().asString(StandardCharsets.UTF_8));
                    recordMap.put("approximateArrivalTimestamp", record.approximateArrivalTimestamp());
                    return recordMap;
                })
                .collect(Collectors.toList());

        return ResponseEntity.ok(response);
    }

    @DeleteMapping("/stream")
    public ResponseEntity<Map<String, String>> deleteStream() {
        kinesisService.deleteStream();

        Map<String, String> response = new HashMap<>();
        response.put("status", "success");
        response.put("message", "Stream deleted successfully");

        return ResponseEntity.ok(response);
    }
}
