package org.example.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DeleteStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

import javax.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Service
public class KinesisService {

    private static final Logger logger = LoggerFactory.getLogger(KinesisService.class);

    @Autowired
    private KinesisClient kinesisClient;

    @Value("${aws.kinesis.stream-name}")
    private String streamName;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public void sendMessage(String partitionKey, Object data) {
        try {
            String jsonData = objectMapper.writeValueAsString(data);

            PutRecordRequest putRecordRequest = PutRecordRequest.builder()
                    .streamName(streamName)
                    .partitionKey(partitionKey)
                    .data(SdkBytes.fromString(jsonData, StandardCharsets.UTF_8))
                    .build();

            PutRecordResponse response = kinesisClient.putRecord(putRecordRequest);
            logger.info("Message sent to stream. Sequence number: {}, Shard ID: {}",
                    response.sequenceNumber(), response.shardId());

        } catch (Exception e) {
            logger.error("Error sending message to Kinesis stream", e);
            throw new RuntimeException("Failed to send message", e);
        }
    }

    public void sendMessageAsync(String partitionKey, Object data) {
        CompletableFuture.runAsync(() -> sendMessage(partitionKey, data));
    }

    public List<Record> readMessages(String shardIteratorType) {
        try {

            ListShardsRequest listShardsRequest = ListShardsRequest.builder().streamName(streamName).build();

            ListShardsResponse listShardsResponse = kinesisClient.listShards(listShardsRequest);
            String shardId = listShardsResponse.shards().get(0).shardId();

            GetShardIteratorRequest shardIteratorRequest = GetShardIteratorRequest.builder()
                    .streamName(streamName).shardId(shardId)
                    .shardIteratorType(ShardIteratorType.fromValue(shardIteratorType))
                    .build();

            GetShardIteratorResponse shardIteratorResponse = kinesisClient.getShardIterator(shardIteratorRequest);
            String shardIterator = shardIteratorResponse.shardIterator();

            GetRecordsRequest getRecordsRequest = GetRecordsRequest.builder()
                    .shardIterator(shardIterator)
                    .limit(10).build();

            GetRecordsResponse getRecordsResponse = kinesisClient.getRecords(getRecordsRequest);
            List<Record> records = getRecordsResponse.records();

            logger.info("Retrieved {} records from stream", records.size());
            return records;

        } catch (Exception e) {
            logger.error("Error reading messages from Kinesis stream", e);
            throw new RuntimeException("Failed to read messages", e);
        }
    }

    public void deleteSteam() {
        kinesisClient.deleteStream(DeleteStreamRequest.builder()
                .streamName(streamName)
                .build());
    }

    public void deleteStream() {
        try {
            DeleteStreamRequest deleteRequest = DeleteStreamRequest.builder()
                    .streamName(streamName).build();

            kinesisClient.deleteStream(deleteRequest);
            logger.info("Stream {} deleted successfully", streamName);

        } catch (Exception e) {
            logger.error("Error deleting stream", e);
            throw new RuntimeException("Failed to delete stream", e);
        }
    }

    private void createStreamIfNotExists() {
        try {
            DescribeStreamRequest describeRequest = DescribeStreamRequest.builder()
                    .streamName(streamName)
                    .build();

            kinesisClient.describeStream(describeRequest);
            logger.info("Stream {} already exists", streamName);

        } catch (ResourceNotFoundException e) {
            // Stream doesn't exist, create it
            logger.info("Creating stream: {}", streamName);

            CreateStreamRequest createRequest = CreateStreamRequest.builder()
                    .streamName(streamName)
                    .shardCount(1)
                    .build();

            kinesisClient.createStream(createRequest);

            waitForStreamToBeActive();
            logger.info("Stream {} created successfully", streamName);
        }
    }

    private void waitForStreamToBeActive() {
        try {
            Thread.sleep(2000); // Wait 2 seconds for LocalStack
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for stream to be active", e);
        }
    }

}
