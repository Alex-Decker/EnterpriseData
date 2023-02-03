package org.example.Reciver;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
public class KafkaReciver implements Supplier<JavaDStream<String>> {

    private final Config config = ConfigFactory.load("application.conf");
    private final List<String> topics;
    private final String boostrap_servers = config.getString("app.kafka.boostrap.servers");
    private final String group_id= config.getString("app.kafka.group.id");
    private final String auto_offset_reset = config.getString("app.kafka.auto.offset.reset");
    private final JavaStreamingContext jsc;

    private final Map<String,Object> kafkaParams = new HashMap<String,Object>(){{
       put("boostrap.servers",boostrap_servers);
       put("key.deserializer", StringDeserializer.class);
       put("value.deserializer", StringDeserializer.class);
        put("group.id",group_id);
        put("auto.offset.reset",auto_offset_reset);
    }};

    @Override
    public JavaDStream<String> get() {
        JavaInputDStream<ConsumerRecord<String,String>> directStream = KafkaUtils.createDirectStream(
                jsc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams)
        );
        JavaDStream<String> javaDStream = directStream.map(ConsumerRecord::value);
        return javaDStream;
    }
}
