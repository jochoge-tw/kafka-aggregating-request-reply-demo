package com.ochoge.messaging.aggregation.domain;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ReciprocalGift {
    public static final String TOPIC = "RequestReply.internal.ReciprocalGift";

    @Builder.Default
    private String type = "Candle";

    @Builder.Default
    private int quantity = 1;

    private Person recipient;

    private Gift originalGift;

    public static ReciprocalGift giftFor(String person, Gift originalGift) {
        return ReciprocalGift.builder()
                .recipient(Person.named(person))
                .originalGift(originalGift)
                .build();
    }
}
