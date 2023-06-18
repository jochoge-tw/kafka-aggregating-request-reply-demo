package com.ochoge.messaging.aggregation.messaging.publisher;

import com.ochoge.messaging.aggregation.messaging.MessagingException;
import com.ochoge.messaging.aggregation.utils.MessageUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;
import java.util.function.BiPredicate;

@Slf4j
@RequiredArgsConstructor
public class CustomHeaderAggregatingReplyReleaseStrategy<K, R> implements BiPredicate<List<ConsumerRecord<K, R>>, Boolean> {
    private final String replyCountHeaderName;

    @Override
    public boolean test(List<ConsumerRecord<K, R>> records, Boolean timeoutExpired) {
        if (records.isEmpty()) {
            return timeoutExpired;
        }
        int aggregateSize = MessageUtils.getHeaderValue(records.get(0).headers(), replyCountHeaderName, Integer::valueOf)
                .orElseThrow(() -> new MessagingException(String.format("Header %s not specified in aggregating request/reply flow", replyCountHeaderName)));
        return records.size() == aggregateSize || timeoutExpired;
    }
}
