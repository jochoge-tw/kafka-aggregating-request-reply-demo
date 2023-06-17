package com.ochoge.messaging.aggregation.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@AllArgsConstructor
@Builder
public class Gift {
    public static final String TOPIC = "RequestReply.internal.Gift";

    @Builder.Default
    private String type = "Candle";

    @Builder.Default
    private int quantity = 1;
    private Person recipient;

    public static Gift giftFor(String person) {
        return Gift.builder()
                .recipient(new Person(person))
                .build();
    }
}
