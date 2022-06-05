package com.tutorial.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class main {

	private final static String TOPIC = "quickstart-events";
	private final static String BOOTSTRAP_SERVERS =
			"localhost:9092,localhost:9093,localhost:9094";

	private final static Logger logger = LoggerFactory.getLogger(main.class);
	
	public static void main(String[] args) {

		logger.info("Start-Pub");

		final Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
				BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,
				"my-transactional-id");

		Producer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());

		producer.initTransactions();
		
		 try {
		     producer.beginTransaction();
		     for (int i = 0; i < 100; i++)
		         producer.send(new ProducerRecord<>(TOPIC, Integer.toString(i), "Java Producer Pub"));
		     producer.commitTransaction();
		 } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
		     // We can't recover from these exceptions, so our only option is to close the producer and exit.
		     producer.close();
		 } catch (KafkaException e) {
		     // For all other exceptions, just abort the transaction and try again.
		     producer.abortTransaction();
		 }
		 
		 producer.close();		
		 
		 logger.info("End-Pub");

	}

}
