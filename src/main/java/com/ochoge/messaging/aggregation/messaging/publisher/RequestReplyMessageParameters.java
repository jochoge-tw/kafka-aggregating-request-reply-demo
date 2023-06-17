package com.ochoge.messaging.aggregation.messaging.publisher;

import com.ochoge.messaging.aggregation.utils.MessageUtils;
import lombok.*;
import org.springframework.kafka.support.KafkaHeaders;

import java.time.Duration;
import java.util.Map;
import java.util.function.Function;

import static java.util.Map.entry;

/**
 * A collection of request-reply message parameters.
 *
 * @param <S> The inbound (request) message type
 * @param <T> The outbound (response) message type
 */
@Data
@NoArgsConstructor(force = true)
@AllArgsConstructor
@Builder
public class RequestReplyMessageParameters<S, T> {

    @NonNull
    private String requestTopic;

    @NonNull
    @Builder.Default
    private Function<S, String> requestMessageKeyFunction = MessageUtils::getNameUuidFor;

    @NonNull
    @Builder.Default
    private Function<S, String> requestCorrelationIdFunction = MessageUtils::getHashBasedUuidFor;

    @NonNull
    private String requestTag;

    /**
     * Timeout for the "send" (or request) leg of the request-reply operation.
     */
    @Builder.Default
    private Duration requestTimeout = Duration.ofSeconds(2);

    @NonNull
    private String replyTopic;

    @NonNull
    @Builder.Default
    private Duration replyTimeout = Duration.ofSeconds(10);

    @NonNull
    private Class<T> replyTypeReference;

    @NonNull
    @Builder.Default
    private Integer replyExpectedCount = 1;

    public Map<String, String> toMessageHeaders(S requestPayload) {
        return Map.ofEntries(
            entry(KafkaHeaders.TOPIC, requestTopic),
            entry(KafkaHeaders.REPLY_TOPIC, replyTopic),
            entry(KafkaHeaders.KEY, requestMessageKeyFunction.apply(requestPayload)),
            entry(KafkaHeaders.CORRELATION_ID, requestCorrelationIdFunction.apply(requestPayload)),
            entry(KafkaHeaders.TIMESTAMP, String.valueOf(System.currentTimeMillis()))
        );
    }
}

