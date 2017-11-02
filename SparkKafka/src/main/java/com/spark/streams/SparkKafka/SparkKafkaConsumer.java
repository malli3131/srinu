package com.spark.streams.SparkKafka;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

public class SparkKafkaConsumer {
	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf().setAppName("Spark kafka Consumer Example").setMaster("local[2]");
		JavaStreamingContext jsc = new JavaStreamingContext(conf, new Duration(5000));
		jsc.sparkContext().setLogLevel("WARN");

		Map<String, Object> kafkaParams = new HashMap<String, Object>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaParams.put("group.id", "Spark-Kafka-Weather-Consumer");
		kafkaParams.put("auto.offset.reset", "latest");

		Collection<String> topics = Arrays.asList("weather");

		JavaInputDStream<ConsumerRecord<Object, Object>> messages = KafkaUtils.createDirectStream(jsc,
				LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topics, kafkaParams));
		JavaDStream<String> strMessages = messages.map(record -> record.value().toString());
		strMessages.foreachRDD(messageRDD -> {
			if (!messageRDD.isEmpty()) {
				messageRDD.foreachPartition(partition -> {
					partition.forEachRemaining(record -> {
						String[] cols = record.split("\\t");
						if (cols.length == 3) {
							int temp = Integer.valueOf(cols[2]);
							if (temp > 40) {
								System.out.println("The day " + new Date(Long.valueOf(cols[1])) + " in city " + cols[0]
										+ " is sunny");
							} else if (temp < 25) {
								System.out.println("The day " + new Date(Long.valueOf(cols[1])) + " in city " + cols[0]
										+ " is cloudy");
							} else {
								System.out.println("The day " + new Date(Long.valueOf(cols[1])) + " in city " + cols[0]
										+ " is normal");
							}
						}
					});
				});
			}
		});
		jsc.start();
		jsc.awaitTermination();
	}
}
