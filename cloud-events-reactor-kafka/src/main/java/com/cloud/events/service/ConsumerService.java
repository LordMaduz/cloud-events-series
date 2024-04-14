package com.cloud.events.service;

import com.cloud.events.model.Event;
import com.cloud.events.model.EventType;
import com.cloud.events.util.CloudEventUtil;
import com.reactive.kafka.ProtoEventProto;

import io.cloudevents.CloudEvent;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.stereotype.Component;

import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;

@Component
@Slf4j
@RequiredArgsConstructor
public class ConsumerService {

    private final ReactiveKafkaConsumerTemplate<String, CloudEvent> reactiveKafkaConsumerTemplate;
    private final CloudEventUtil cloudEventUtils;
    @PostConstruct
    public void initialize() {
        onNEventReceived();
    }


    public Disposable onNEventReceived() {
        return reactiveKafkaConsumerTemplate
                .receive()
                .doOnNext(consumerRecord -> log.info("received key={}, value={} from topic={}, offset={}",
                        consumerRecord.key(), consumerRecord.value(), consumerRecord.topic(), consumerRecord.offset())
                )
                .concatMap(this::processRecord)
                .doOnNext(event -> {
                    log.info("successfully consumed {}={}", Event.class.getSimpleName(), event);
                }).subscribe(record-> {
                    record.receiverOffset().commit();
                });
    }

    private Mono<ReceiverRecord<String, CloudEvent>> processRecord(ReceiverRecord<String, CloudEvent> record) {
        CloudEvent cloudEvent = record.value();
        final String eventClass = cloudEvent.getType();
        if (EventType.JSON.toString().equals(eventClass) || EventType.BINARY.toString().equals(eventClass)) {
            Event event = cloudEventUtils.toObject(record.value(), Event.class);
            log.info("Event Record Processed: {}", event);
        } else if(EventType.PROTO.toString().equals(eventClass)) {
            ProtoEventProto.ProtoEvent event = cloudEventUtils.toObjectFromProto(record.value(), ProtoEventProto.ProtoEvent.class);
            log.info("Event Record Processed: {}", event);
        }
        return Mono.just(record);
    }

}
