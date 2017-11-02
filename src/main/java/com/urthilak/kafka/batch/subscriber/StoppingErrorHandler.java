package com.urthilak.kafka.batch.subscriber;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.BatchErrorHandler;

@Configuration
public class StoppingErrorHandler implements BatchErrorHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(StoppingErrorHandler.class);

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Override
    public void handle(Exception thrownException, ConsumerRecords<?, ?> data) {
        LOGGER.error("Stopping subscription on the back of error {}", thrownException.getMessage());
        kafkaListenerEndpointRegistry.stop();
    }
}
