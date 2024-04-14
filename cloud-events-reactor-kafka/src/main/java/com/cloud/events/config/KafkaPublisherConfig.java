package com.cloud.events.config;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.message.Encoding;
import io.cloudevents.jackson.JsonFormat;
import io.cloudevents.kafka.CloudEventSerializer;
import io.cloudevents.protobuf.ProtobufFormat;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.util.Map;

@Configuration
public class KafkaPublisherConfig extends KafkaBasicConfig {


    private SenderOptions<String, CloudEvent> getStructuredSenderOptions() {
        Map<String, Object> props = getBasicConfig();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CloudEventSerializer.class);
        props.put(CloudEventSerializer.ENCODING_CONFIG, Encoding.STRUCTURED);
        props.put(CloudEventSerializer.EVENT_FORMAT_CONFIG, JsonFormat.CONTENT_TYPE);


        return SenderOptions.create(props);
    }

    private SenderOptions<String, CloudEvent> getStructuredProtoBufSenderOptions() {
        Map<String, Object> props = getBasicConfig();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CloudEventSerializer.class);
        props.put(CloudEventSerializer.ENCODING_CONFIG, Encoding.STRUCTURED);
        props.put(CloudEventSerializer.EVENT_FORMAT_CONFIG, ProtobufFormat.PROTO_CONTENT_TYPE);


        return SenderOptions.create(props);
    }

    private SenderOptions<String, CloudEvent> getBinarySenderOptions() {
        Map<String, Object> props = getBasicConfig();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CloudEventSerializer.class);
        props.put(CloudEventSerializer.ENCODING_CONFIG, Encoding.BINARY);

        return SenderOptions.create(props);
    }

    @Bean(value = "structuredKafkaSender")
    public KafkaSender<String, CloudEvent> structuredKafkaSender() {
        return KafkaSender.create(getStructuredSenderOptions());
    }

    @Bean(value = "structuredProtoBufKafkaSender")
    public KafkaSender<String, CloudEvent> structuredProtoBufKafkaSender() {
        return KafkaSender.create(getStructuredSenderOptions());
    }

    @Bean(value = "binaryKafkaSender")
    public KafkaSender<String, CloudEvent> binaryKafkaSender() {
        return KafkaSender.create(getBinarySenderOptions());
    }

    @Bean(value = "structuredReactiveKafkaProducerTemplate")
    public ReactiveKafkaProducerTemplate<String, CloudEvent> structuredReactiveKafkaProducerTemplate() {
        return new ReactiveKafkaProducerTemplate<>(getStructuredSenderOptions());
    }

    @Bean(value = "structuredProtoBufReactiveKafkaProducerTemplate")
    public ReactiveKafkaProducerTemplate<String, CloudEvent> structuredProtoBufReactiveKafkaProducerTemplate() {
        return new ReactiveKafkaProducerTemplate<>(getStructuredProtoBufSenderOptions());
    }

    @Bean(value = "binaryReactiveKafkaProducerTemplate")
    public ReactiveKafkaProducerTemplate<String, CloudEvent> binaryReactiveKafkaProducerTemplate() {
        return new ReactiveKafkaProducerTemplate<>(getBinarySenderOptions());
    }
}
