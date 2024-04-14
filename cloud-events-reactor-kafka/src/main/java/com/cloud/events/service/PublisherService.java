package com.cloud.events.service;

import com.cloud.events.model.Event;
import com.cloud.events.util.CloudEventUtil;
import com.reactive.kafka.ProtoEventProto;
import com.reactive.kafka.mapper.EventProtoMapper;
import com.reactive.kafka.model.ProtoEvent;

import io.cloudevents.CloudEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Service;
import reactor.kafka.sender.SenderRecord;

import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class PublisherService {

    @Value("${KAFKA_TOPIC_NAME}")
    public String kafkaTopicName;
    private final CloudEventUtil cloudEventUtil;
    private final EventProtoMapper protoMapper;
    private final ReactiveKafkaProducerTemplate<String, CloudEvent> binaryReactiveKafkaProducerTemplate;

    private final ReactiveKafkaProducerTemplate<String, CloudEvent> structuredReactiveKafkaProducerTemplate;

    private final ReactiveKafkaProducerTemplate<String, CloudEvent> structuredProtoBufReactiveKafkaProducerTemplate;

    public void publishStructuredPOJOEvent(final String key) {
        final Event event = Event.builder().eventName("Kafka Event").build();
        final CloudEvent cloudEvent = cloudEventUtil.pojoCloudEvent(event, UUID.randomUUID().toString());

        structuredReactiveKafkaProducerTemplate.send(SenderRecord.create(new ProducerRecord<>(
                        kafkaTopicName,
                        key, cloudEvent), new Object()))
                .doOnError(error -> log.info("unable to send message due to: {}", error.getMessage())).subscribe(record -> {
                    RecordMetadata metadata = record.recordMetadata();
                    log.info("send message with partition: {} offset: {}", metadata.partition(), metadata.offset());
                });
    }

    public void publishStructuredProtoBufEvent(final String key) {
        final ProtoEvent protoEventPojo = ProtoEvent.builder().eventId("1").eventName("protoEvent").build();
        final ProtoEventProto.ProtoEvent protoEvent = protoMapper.toProto(protoEventPojo);
        final CloudEvent cloudEvent = cloudEventUtil.protoCloudEvent(protoEvent, UUID.randomUUID().toString());

        structuredProtoBufReactiveKafkaProducerTemplate.send(SenderRecord.create(new ProducerRecord<>(
                        kafkaTopicName,
                        key, cloudEvent), new Object()))
                .doOnError(error -> log.info("unable to send message due to: {}", error.getMessage())).subscribe(record -> {
                    RecordMetadata metadata = record.recordMetadata();
                    log.info("send message with partition: {} offset: {}", metadata.partition(), metadata.offset());
                });
    }

    public void publishStructuredJSONEvent(final String key) {
        final Event event = Event.builder().eventName("Kafka Event").build();
        final CloudEvent cloudEvent = cloudEventUtil.jsonCloudEvent(event, UUID.randomUUID().toString());

        structuredReactiveKafkaProducerTemplate.send(SenderRecord.create(new ProducerRecord<>(
                        kafkaTopicName,
                        key, cloudEvent), new Object()))
                .doOnError(error -> log.info("unable to send message due to: {}", error.getMessage())).subscribe(record -> {
                    RecordMetadata metadata = record.recordMetadata();
                    log.info("send message with partition: {} offset: {}", metadata.partition(), metadata.offset());
                });
    }

    public void publishBinaryEvent(final String key){
        final ProtoEvent protoEventPojo = ProtoEvent.builder().eventId("eventId").eventName("protoEvent").build();
        final ProtoEventProto.ProtoEvent protoEvent = protoMapper.toProto(protoEventPojo);
        final CloudEvent cloudEvent = cloudEventUtil.bytesCloudEvent(protoEvent, UUID.randomUUID().toString());

        binaryReactiveKafkaProducerTemplate.send(SenderRecord.create(new ProducerRecord<>(
                        kafkaTopicName,
                        key, cloudEvent), new Object()))
                .doOnError(error -> log.info("unable to send message due to: {}", error.getMessage())).subscribe(record -> {
                    RecordMetadata metadata = record.recordMetadata();
                    log.info("send message with partition: {} offset: {}", metadata.partition(), metadata.offset());
                });
    }
}
