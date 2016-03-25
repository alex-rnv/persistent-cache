package com.alexrnv.datastream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created: 2/24/16 10:41 AM
 * Author: alex
 */
@Component
@ConditionalOnExpression("'${mode}'=='kafka'")
public class KafkaWriter extends WriterWithStats {

    @Value("${out.host}")
    private String host;
    @Value("${out.port}")
    private int port;

    @Value("${kafka.topic}")
    private String topic;

    private KafkaProducer<Long, String> kafkaProducer;
    private final AtomicLong msgId = new AtomicLong();

    @PostConstruct
    protected void init() throws IOException {
        Properties props = new Properties();
        props.put("bootstrap.servers", host+":"+port);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.kafkaProducer = new KafkaProducer<>(props);

        super.init();
    }


    @Override
    public void write(String line) {
        incWriteCounter();
        kafkaProducer.send(new ProducerRecord<>(topic, msgId.getAndIncrement(), line));
    }

    @Override
    protected void finalize() throws Throwable {
        if(kafkaProducer != null) {
            kafkaProducer.close();
        }
        super.finalize();
    }
}
