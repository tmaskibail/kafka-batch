package com.urthilak.kafka.batch.subscriber;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class CustomKafkaListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomKafkaListener.class);


    @KafkaListener(id = "CG1", topics = "test", containerFactory = "kafkaListenerContainerFactory")
    public void listen(List<ConsumerRecord<Integer, String>> records, Acknowledgment acknowledgment) {

//        for (ConsumerRecord record : records) {
//            LOGGER.info("Batch size {}, Record {}, Offset {}, partition {}", records.size(), record.key(), record.offset(), record.partition());
//        }

//        throw new RuntimeException("Custom Error!");

        LOGGER.info("Batch size {}", records.size());
        acknowledgment.acknowledge();

    }
}
