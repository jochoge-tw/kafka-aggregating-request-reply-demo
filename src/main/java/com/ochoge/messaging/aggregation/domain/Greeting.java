package com.ochoge.messaging.aggregation.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.Locale;

@Data
@Builder
@AllArgsConstructor
public class Greeting {
    public static final String TOPIC = "RequestReply.internal.Greetings";
    private Person recipient;

    @Builder.Default
    private String greeting = "Hello";

    @Builder.Default
    private String language = Locale.ENGLISH.getLanguage();

    @Builder.Default
    private Instant timestamp = Instant.now();

    public static Greeting defaultTo(String recipient) {
        return Greeting.builder()
                .recipient(Person.named(recipient))
                .build();
    }
}
