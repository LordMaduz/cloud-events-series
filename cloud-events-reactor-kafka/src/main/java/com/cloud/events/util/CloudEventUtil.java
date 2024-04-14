package com.cloud.events.util;

import static io.cloudevents.core.CloudEventUtils.mapData;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.reactive.kafka.mapper.EventProtoMapper;

import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventData;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.data.BytesCloudEventData;
import io.cloudevents.core.data.PojoCloudEventData;
import io.cloudevents.jackson.JsonCloudEventData;
import io.cloudevents.jackson.PojoCloudEventDataMapper;
import io.cloudevents.protobuf.ProtoCloudEventData;
import io.cloudevents.spring.http.CloudEventHttpUtils;
import lombok.RequiredArgsConstructor;

import org.apache.commons.lang3.SerializationUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.Objects;

@Component
@RequiredArgsConstructor
public class CloudEventUtil {

    private final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final String PARTITIONING_KEY_EXTENSION = "partitionkey";
    private final String URL = "/api";
    private final EventProtoMapper eventProtoMapper;

    public <T extends Message> T toObjectFromProto(CloudEvent cloudEvent, Class<T> type) {
        try {
            if (cloudEvent.getData() instanceof ProtoCloudEventData protoCloudEventData) {
                final Message message = protoCloudEventData.getMessage();
                return ((Any) message).unpack(type);
            } else {
                throw new RuntimeException("Unsupported Cloud Event Data Type");
            }

        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    public <T> T toObject(CloudEvent cloudEvent, Class<T> type) {
        try {
            if (cloudEvent.getData() instanceof JsonCloudEventData) {
                return OBJECT_MAPPER.treeToValue(((JsonCloudEventData) cloudEvent.getData()).getNode(), type);
            } else if (cloudEvent.getData() instanceof PojoCloudEventData) {
                return Objects.requireNonNull(mapData(cloudEvent, PojoCloudEventDataMapper.from(OBJECT_MAPPER, type)))
                    .getValue();
            } else if (cloudEvent.getData() instanceof BytesCloudEventData bytesCloudEventData) {
                return SerializationUtils.deserialize(bytesCloudEventData.toBytes());
            } else {
                throw new RuntimeException("Unsupported Cloud Event Data Type");
            }

        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private <T> CloudEventData pojoCloudEventData(T object) {
        return PojoCloudEventData.wrap(object, OBJECT_MAPPER::writeValueAsBytes);
    }

    private <T extends Serializable> CloudEventData bytesCloudEventData(T object) {
        return BytesCloudEventData.wrap(Objects.requireNonNull(SerializationUtils.serialize(object)));
    }

    private <T> CloudEventData jsonCloudEventData(T object) {
        return JsonCloudEventData.wrap(OBJECT_MAPPER.valueToTree(object));
    }

    public <T extends Message> CloudEventData protoCloudEventData(T object) {
        return ProtoCloudEventData.wrap(object);
    }

    public <T extends Message> CloudEvent protoCloudEvent(final T object, final String id) {
        return cloudEventBuilder(object, id).withData(protoCloudEventData(object))
            .build();
    }

    public <T> CloudEvent pojoCloudEvent(final T object, final String id) {
        return cloudEventBuilder(object, id).withData(pojoCloudEventData(object))
            .build();
    }

    public <T extends Serializable> CloudEvent bytesCloudEvent(final T object, final String id) {
        return cloudEventBuilder(object, id).withData(bytesCloudEventData(object))
            .build();
    }

    public <T> CloudEvent pojoCloudEvent(final CloudEvent cloudEvent, final T object, final String id) {
        return cloudEventBuilder(cloudEvent, object, id).withData(pojoCloudEventData(object))
            .build();
    }

    public <T> CloudEvent pojoCloudEvent(final HttpHeaders httpHeaders, final T object, final String id) {
        return cloudEventBuilder(httpHeaders, object, id).withData(pojoCloudEventData(object))
            .build();
    }

    public <T> CloudEvent jsonCloudEvent(final T object, final String id) {
        return cloudEventBuilder(object, id).withData(jsonCloudEventData(object))
            .build();
    }

    public <T> CloudEvent jsonCloudEvent(final CloudEvent cloudEvent, final T object, final String id) {
        return cloudEventBuilder(object, id).withData(jsonCloudEventData(object))
            .build();
    }

    public <T> CloudEvent jsonCloudEvent(final HttpHeaders httpHeaders, final T object, final String id) {
        return cloudEventBuilder(httpHeaders, object, id).withData(jsonCloudEventData(object))
            .build();
    }

    private <T extends Serializable> CloudEventBuilder cloudEventBuilder(final T object, final String id) {
        return CloudEventBuilder.v1()
            .withSource(URI.create(URL))
            .withType(object.getClass()
                .getName())
            .withId(id)
            .withExtension(PARTITIONING_KEY_EXTENSION, id)
            .withTime(ZonedDateTime.now()
                .toOffsetDateTime());
    }

    private <T extends Message> CloudEventBuilder cloudEventBuilder(final T object, final String id) {
        return CloudEventBuilder.v1()
            .withSource(URI.create(URL))
            .withType(object.getClass()
                .getName())
            .withId(id)
            .withExtension(PARTITIONING_KEY_EXTENSION, id)
            .withTime(ZonedDateTime.now()
                .toOffsetDateTime());
    }

    private <T> CloudEventBuilder cloudEventBuilder(final T object, final String id) {
        return CloudEventBuilder.v1()
            .withSource(URI.create(URL))
            .withType(object.getClass()
                .getName())
            .withId(id)
            .withExtension(PARTITIONING_KEY_EXTENSION, id)
            .withTime(ZonedDateTime.now()
                .toOffsetDateTime());
    }

    private <T> CloudEventBuilder cloudEventBuilder(final CloudEvent cloudEvent, final T object, String id) {
        return CloudEventBuilder.from(cloudEvent)
            .withSource(URI.create(URL))
            .withId(id)
            .withType(object.getClass()
                .getName());

    }

    private <T> CloudEventBuilder cloudEventBuilder(final HttpHeaders httpHeaders, final T object, String id) {
        return CloudEventHttpUtils.fromHttp(httpHeaders)
            .withSource(URI.create(URL))
            .withId(id)
            .withType(object.getClass()
                .getTypeName());
    }
}
