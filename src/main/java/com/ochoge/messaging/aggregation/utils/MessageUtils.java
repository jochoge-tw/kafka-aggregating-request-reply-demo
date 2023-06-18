package com.ochoge.messaging.aggregation.utils;

import lombok.experimental.UtilityClass;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.StreamSupport;

@UtilityClass
public class MessageUtils {

    public static List<byte[]> getHeaderValues(Headers headers, String headerKey) {
        return StreamSupport.stream(headers.headers(headerKey).spliterator(), false)
                .map(Header::value)
                .toList();
    }

    public static Optional<byte[]> getHeaderValue(Headers headers, String headerKey) {
        return getHeaderValues(headers, headerKey).stream().findFirst();
    }

    public static <T> List<T> getHeaderValues(Headers headers, String headerKey, Function<String, T> transformer) {
        return getHeaderValues(headers, headerKey)
                .stream()
                .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
                .map(transformer)
                .toList();
    }

    public static <T> Optional<T> getHeaderValue(Headers headers, String headerKey, Function<String, T> transformer) {
        return getHeaderValue(headers, headerKey)
                .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
                .map(transformer);
    }

    public static <T> String getNameUuidFor(T object) {
        return UUID.nameUUIDFromBytes(object.toString().getBytes(StandardCharsets.UTF_8)).toString();
    }

    public static <T> String getHashBasedUuidFor(T object) {
        String stringifiedHash = Long.toHexString(object.hashCode());
        return UUID.nameUUIDFromBytes(stringifiedHash.getBytes(StandardCharsets.UTF_8)).toString();
    }
}
