package com.ochoge.messaging.aggregation.service;

import com.ochoge.messaging.aggregation.domain.GreetingReply;
import com.ochoge.messaging.aggregation.messaging.publisher.RequestReplyMessageParameters;
import com.ochoge.messaging.aggregation.messaging.publisher.RequestReplyMessagePublisher;
import com.ochoge.messaging.aggregation.domain.Greeting;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class GreetingService {
    private final RequestReplyMessagePublisher messagePublisher;

    public List<GreetingReply> sendAndAwait(Greeting greeting) {
        RequestReplyMessageParameters<Greeting, GreetingReply> parameters = RequestReplyMessageParameters.<Greeting, GreetingReply>builder()
                .requestTopic(Greeting.TOPIC)
                .requestTimeout(Duration.ofSeconds(5))
                .requestTag("Greetings")
                .replyTopic(GreetingReply.TOPIC)
                .replyTimeout(Duration.ofSeconds(10))
                .replyTypeReference(GreetingReply.class)
                .replyExpectedCount(1)
                .build();
        return messagePublisher.publishWithReply(greeting, parameters);
    }
}
