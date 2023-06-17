package com.ochoge.messaging.aggregation.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.time.Instant;

@Data
@AllArgsConstructor
@Builder
public class GreetingReply {
    public static final String TOPIC = "RequestReply.internal.Replies";
    private Greeting originalGreeting;
    private String reply;

    @Builder.Default
    private Instant timestamp = Instant.now();

    public static GreetingReply reply(String reply, Greeting greeting) {
        return GreetingReply.builder()
                .originalGreeting(greeting)
                .reply(reply)
                .build();
    }
}
