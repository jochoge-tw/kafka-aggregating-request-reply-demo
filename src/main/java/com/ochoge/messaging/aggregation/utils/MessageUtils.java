package com.ochoge.messaging.aggregation.utils;

import lombok.experimental.UtilityClass;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.StreamSupport;
import org.apache.kafka.common.header.Header;

@UtilityClass
public class MessageUtils {

    public static <K, V> List<byte[]> getHeaderValues(ConsumerRecord<K, V> consumerRecord, String headerKey) {
        return StreamSupport.stream(consumerRecord.headers().headers(headerKey).spliterator(), false)
                .map(Header::value)
                .toList();
    }

    public static <K, V> Optional<byte[]> getHeaderValue(ConsumerRecord<K, V> consumerRecord, String headerKey) {
        return getHeaderValues(consumerRecord, headerKey).stream().findFirst();
    }

    public static <T> String getNameUuidFor(T object) {
        return UUID.nameUUIDFromBytes(object.toString().getBytes(StandardCharsets.UTF_8)).toString();
    }

    public static <T> String getHashBasedUuidFor(T object) {
        String stringifiedHash = Long.toHexString(object.hashCode());
        return UUID.nameUUIDFromBytes(stringifiedHash.getBytes(StandardCharsets.UTF_8)).toString();
    }
}
