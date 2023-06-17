package com.ochoge.messaging.aggregation.service;

import com.ochoge.messaging.aggregation.messaging.publisher.RequestReplyMessageParameters;
import com.ochoge.messaging.aggregation.messaging.publisher.RequestReplyMessagePublisher;
import com.ochoge.messaging.aggregation.domain.Gift;
import com.ochoge.messaging.aggregation.domain.ReciprocalGift;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class GiftService {
    private final RequestReplyMessagePublisher messagePublisher;

    public List<ReciprocalGift> sendAndAwait(Gift gift) {
        RequestReplyMessageParameters<Gift, ReciprocalGift> parameters = RequestReplyMessageParameters.<Gift, ReciprocalGift>builder()
                .requestTopic(Gift.TOPIC)
                .requestTimeout(Duration.ofSeconds(5))
                .requestTag("Gifts")
                .replyTopic(ReciprocalGift.TOPIC)
                .replyTimeout(Duration.ofSeconds(10))
                .replyTypeReference(ReciprocalGift.class)
                .replyExpectedCount(2)
                .build();
        return messagePublisher.publishWithReply(gift, parameters);
    }
}
