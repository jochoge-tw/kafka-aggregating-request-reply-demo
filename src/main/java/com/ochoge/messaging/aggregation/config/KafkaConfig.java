package com.ochoge.messaging.aggregation.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.requestreply.AggregatingReplyingKafkaTemplate;

import java.util.Collection;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConfig {

    @Value("${request-reply.reply-topics}")
    private String[] repliesTopics;

    @Bean
    public ProducerFactory<String, Object> defaultProducerFactory(
            KafkaProperties kafkaProperties) {
        Map<String, Object> producerProperties = kafkaProperties.buildProducerProperties();
        producerProperties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000L);
        return new DefaultKafkaProducerFactory<>(producerProperties);
    }

    @Bean
    public ConsumerFactory<String, Object> defaultConsumerFactory(
            KafkaProperties kafkaProperties) {
        return new DefaultKafkaConsumerFactory<>(kafkaProperties.buildConsumerProperties());
    }

    @Bean
    @Primary
    public KafkaTemplate<String, Object> defaultKafkaTemplate(
            ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            KafkaTemplate<String, Object> kafkaTemplate) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setReplyTemplate(kafkaTemplate);
        factory.setMissingTopicsFatal(false);
        return factory;
    }

    @Bean
    public ConcurrentMessageListenerContainer<String, Object> repliesContainer(
            ConcurrentKafkaListenerContainerFactory<String, Object> containerFactory) {
        ConcurrentMessageListenerContainer<String, Object> repliesContainer = containerFactory.createContainer(repliesTopics);
        repliesContainer.getContainerProperties().setGroupId("request_reply");
        repliesContainer.setAutoStartup(false);
        return repliesContainer;
    }

    @Bean
    public ContainerProperties containerProperties() {
        ContainerProperties containerProperties = new ContainerProperties(repliesTopics);
        containerProperties.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return containerProperties;
    }

    @Bean
    public ConsumerFactory<String, Collection<ConsumerRecord<String, Object>>> aggregatingConsumerFactory(
            KafkaProperties kafkaProperties) {
        return new DefaultKafkaConsumerFactory<>(kafkaProperties.buildConsumerProperties());
    }

    @Bean
    public KafkaMessageListenerContainer<String, Collection<ConsumerRecord<String, Object>>> aggregatingRepliesContainer(
            ConsumerFactory<String, Collection<ConsumerRecord<String, Object>>> consumerFactory,
            ContainerProperties containerProperties) {
        return new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
    }

    @Bean
    public AggregatingReplyingKafkaTemplate<String, Object, Object> defaultAggregatingReplyingKafkaTemplate(
            ProducerFactory<String, Object> producerFactory,
            KafkaMessageListenerContainer<String, Collection<ConsumerRecord<String, Object>>> listenerContainer) {
        AggregatingReplyingKafkaTemplate<String, Object, Object> template =
                new AggregatingReplyingKafkaTemplate<>(producerFactory, listenerContainer, (list, timeout) -> list.size() == 2 || timeout);
        template.setSharedReplyTopic(true);
        template.start();
        return template;
    }
}
