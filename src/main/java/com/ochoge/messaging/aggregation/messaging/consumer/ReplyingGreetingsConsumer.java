package com.ochoge.messaging.aggregation.messaging.consumer;

import com.ochoge.messaging.aggregation.domain.Greeting;
import com.ochoge.messaging.aggregation.domain.GreetingReply;
import com.ochoge.messaging.aggregation.messaging.CustomHeaders;
import com.ochoge.messaging.aggregation.utils.MessageUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class ReplyingGreetingsConsumer {
    private final ObjectMapper objectMapper;

    @SneakyThrows
    @KafkaListener(topics = Greeting.TOPIC, batch = "false")
    @SendTo
    public List<Message<GreetingReply>> consumer(ConsumerRecord<String, Object> consumerRecord) {
        Object rawGreeting = consumerRecord.value();
        log.info("Received message {}", rawGreeting);
        Greeting greeting = objectMapper.readValue(String.valueOf(rawGreeting), Greeting.class);
        return List.of(
//                createMessage(GreetingReply.reply("Hola, comestas", greeting), consumerRecord),
                createMessage(GreetingReply.reply("Bonjour", greeting), consumerRecord));
    }

    private Message<GreetingReply> createMessage(GreetingReply reply, ConsumerRecord<String, Object> consumerRecord) {
        Map<String, Object> headers = new HashMap<>();
        MessageUtils.getHeaderValues(consumerRecord.headers(), KafkaHeaders.CORRELATION_ID).forEach(headerValue -> headers.put(KafkaHeaders.CORRELATION_ID, headerValue));
        headers.put(KafkaHeaders.KEY, MessageUtils.getHashBasedUuidFor(reply));
        headers.put(KafkaHeaders.TIMESTAMP, System.currentTimeMillis());
        headers.put(KafkaHeaders.TOPIC, GreetingReply.TOPIC);
        MessageUtils.getHeaderValue(consumerRecord.headers(), CustomHeaders.EXPECTED_REPLIES_COUNT).ifPresent(headerValue -> headers.put(CustomHeaders.EXPECTED_REPLIES_COUNT, headerValue));

        return MessageBuilder.withPayload(reply)
                .copyHeaders(headers)
                .build();
    }
}
