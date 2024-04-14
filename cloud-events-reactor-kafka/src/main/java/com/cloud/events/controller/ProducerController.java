package com.cloud.events.controller;


import com.cloud.events.service.PublisherService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping
@RequiredArgsConstructor
public class ProducerController {

    private final PublisherService publisherService;


    @GetMapping
    public Mono<String> test() {
        publisherService.publishStructuredJSONEvent("TEST");
        publisherService.publishStructuredPOJOEvent("TEST");
        publisherService.publishBinaryEvent("TEST");
        publisherService.publishStructuredProtoBufEvent("TEST");
        return Mono.just("HELLO");
    }
}
