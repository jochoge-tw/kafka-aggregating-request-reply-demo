package com.ochoge.messaging.aggregation.messaging.consumer;

import com.ochoge.messaging.aggregation.domain.Gift;
import com.ochoge.messaging.aggregation.domain.ReciprocalGift;
import com.ochoge.messaging.aggregation.utils.MessageUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Slf4j
@Component
@RequiredArgsConstructor
public class ReplyingGiftConsumer {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @SneakyThrows
    @KafkaListener(topics = Gift.TOPIC, batch = "false")
    public void consumer(ConsumerRecord<String, Object> consumerRecord) {
        Object rawGift = consumerRecord.value();
        log.info("Received message {}", rawGift);
        Gift gift = objectMapper.readValue(String.valueOf(rawGift), Gift.class);

        ProducerRecord<String, Object> producerRecord = createProducerRecord(ReciprocalGift.giftFor("Josh", gift), consumerRecord);
        kafkaTemplate.send(producerRecord);

        producerRecord = createProducerRecord(ReciprocalGift.giftFor("Jason", gift), consumerRecord);
        kafkaTemplate.send(producerRecord);
    }

    private ProducerRecord<String, Object> createProducerRecord(ReciprocalGift reciprocalGift, ConsumerRecord<String, Object> consumerRecord) {
        ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(ReciprocalGift.TOPIC, reciprocalGift);
        MessageUtils.getHeaderValues(consumerRecord, KafkaHeaders.CORRELATION_ID).forEach(headerValue -> producerRecord.headers().add(KafkaHeaders.CORRELATION_ID, headerValue));
        producerRecord.headers().add(KafkaHeaders.KEY, MessageUtils.getHashBasedUuidFor(reciprocalGift).getBytes(StandardCharsets.UTF_8));
        producerRecord.headers().add(KafkaHeaders.TIMESTAMP, String.valueOf(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8));
        producerRecord.headers().add(KafkaHeaders.TOPIC, ReciprocalGift.TOPIC.getBytes(StandardCharsets.UTF_8));
        return producerRecord;
    }
}