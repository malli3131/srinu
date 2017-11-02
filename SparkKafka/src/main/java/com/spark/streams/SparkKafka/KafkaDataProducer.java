package com.spark.streams.SparkKafka;

import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaDataProducer {
	public static void main(String[] args) throws InterruptedException {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		String topic = "weather";

		String[] citis = { "Hyderabad", "Chennai", "Bangalore", "Mumbai", "Delhi", "Vizag" };
		int min = 20;
		int max = 50;
		Random rand = new Random();

		int low_index = 0;
		int high_index = 6;
		@SuppressWarnings("resource")
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
		while (true) {
			String city = citis[rand.nextInt(high_index - low_index) + low_index];
			int temp = (rand.nextInt(max - min) + min);
			String record = city + "\t" + System.currentTimeMillis() + "\t" + temp;
			ProducerRecord<String, String> kafkaRecord = new ProducerRecord<String, String>(topic, city, record);
			// System.out.println(kafkaRecord);
			producer.send(kafkaRecord);
			Thread.sleep(2000);
		}
	}
}
