package com.reactive.kafka.model;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class ProtoEvent {
    private String eventId;
    private String eventName;
}
