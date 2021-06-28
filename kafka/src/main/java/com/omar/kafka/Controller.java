package com.omar.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Properties;

@Slf4j
@RestController
@RequestMapping("/test")
@CrossOrigin("*")
public class Controller {

    @PostMapping
    public String cfsbSendDailyFiles() {
        log.info("CFSB DAILY FILES SENDING STARTED");
        Instant start = Instant.now();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream("quickstart-events");
/*
        log.info("COMIENZO");
        textLines.foreach((line, sas) -> {
            log.info("{} sdsdsdsds {}", line, sas);
        });
        log.info("FIN");*/


        KTable<String, Long> wordCounts = textLines
                .flatMapValues(line -> Arrays.asList(line.toLowerCase().split(" ")))
                .groupBy((keyIgnored, word) -> word)
                .count();

        wordCounts.toStream().foreach((line, sas) -> {
            log.info("w:{} c:{}", line, sas);
        });

        //wordCounts.toStream().to("output-topic", Produced.with(Serdes.String(), Serdes.Long()));
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Instant end = Instant.now();
        log.info("CFSB DAILY FILES SENDING FINISHED");
        return String.valueOf(Duration.between(start, end));
    }
}
