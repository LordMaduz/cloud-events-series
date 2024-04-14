package com.reactive.kafka.mapper;

import com.reactive.kafka.ProtoEventProto;
import com.reactive.kafka.model.ProtoEvent;
import org.springframework.stereotype.Component;

@Component
public class EventProtoMapper implements ProtobufMapper<ProtoEvent, ProtoEventProto.ProtoEvent>{
    @Override
    public Class<ProtoEvent> getJavaClassType() {
        return ProtoEvent.class;
    }

    @Override
    public Class<ProtoEventProto.ProtoEvent> getProtoClassType() {
        return ProtoEventProto.ProtoEvent.class;
    }

    @Override
    public ProtoEventProto.ProtoEvent toProto(ProtoEvent protoEvent) {
        ProtoEventProto.ProtoEvent.Builder builder = ProtoEventProto.ProtoEvent.newBuilder();
        setIfNotNull(protoEvent::getEventId,builder::setEventId);
        setIfNotNull(protoEvent::getEventName,builder::setEventName);
        return builder.build();
    }

    @Override
    public ProtoEvent fromProto(ProtoEventProto.ProtoEvent proto) {
        ProtoEvent protoEvent = ProtoEvent.builder()
                .eventName(proto.getEventId())
                .build();
        return protoEvent;
    }
}
