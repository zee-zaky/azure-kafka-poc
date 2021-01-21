/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs.samples.SimpleReceive;

import com.azure.core.amqp.AmqpTransportType;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventProcessorClient;
import com.azure.messaging.eventhubs.EventProcessorClientBuilder;
import com.azure.messaging.eventhubs.checkpointstore.blob.BlobCheckpointStore;
import com.azure.messaging.eventhubs.models.ErrorContext;
import com.azure.messaging.eventhubs.models.EventContext;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobContainerClientBuilder;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.function.Consumer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.config.SaslConfigs;

import java.util.Properties;

public class SimpleReceive {
	private static final String EH_NAMESPACE_CONNECTION_STRING = "Endpoint=sb://ehubns-kafkapoc-zaky.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=8IEM/bAExPyMKi1ItJA+SIe+UAZpbcO5B5Jz5ztCtW4=";
	private static final String eventHubName = "ehub-kafkapoc-zaky";
	private static final String STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=storagekafkapoczaky;AccountKey=6vhwa60hiPYNWvwbrO9EslEvgez+DtDO8dYmFtetlLd6vyAz1AEHC8mFCK9HNI5ujOj2ETE0mzyYCgGkEIQZHg==";
	private static final String STORAGE_CONTAINER_NAME = "messages";

	// Create the Properties class to instantiate the Consumer with the desired settings:
	private static Properties Kafka_Props = new Properties();
	private static final String Kafka_Topic = "test";
	private static KafkaProducer<String, String> Kafka_Producer = null;


	public static final Consumer<EventContext> PARTITION_PROCESSOR = eventContext -> {
		System.out.printf("Processing event from partition %s with sequence number %d with body: %s %n",
				eventContext.getPartitionContext().getPartitionId(), eventContext.getEventData().getSequenceNumber(), eventContext.getEventData().getBodyAsString());

		Kafka_Producer.send(new ProducerRecord<String, String>(Kafka_Topic, eventContext.getEventData().getBodyAsString()));

		if (eventContext.getEventData().getSequenceNumber() % 10 == 0) {
			eventContext.updateCheckpoint();
		}
	};

	public static final Consumer<ErrorContext> ERROR_HANDLER = errorContext -> {
		System.out.printf("Error occurred in partition processor for partition %s, %s.%n",
				errorContext.getPartitionContext().getPartitionId(),
				errorContext.getThrowable());
	};

	public static void main(String[] args) throws Exception {
		BlobContainerAsyncClient blobContainerAsyncClient = new BlobContainerClientBuilder()
				.connectionString(STORAGE_CONNECTION_STRING)
				.containerName(STORAGE_CONTAINER_NAME)
				.buildAsyncClient();

		EventProcessorClientBuilder eventProcessorClientBuilder = new EventProcessorClientBuilder()
				.connectionString(EH_NAMESPACE_CONNECTION_STRING, eventHubName)
				.consumerGroup(EventHubClientBuilder.DEFAULT_CONSUMER_GROUP_NAME)
				.processEvent(PARTITION_PROCESSOR)
				.processError(ERROR_HANDLER)
				.transportType(AmqpTransportType.AMQP_WEB_SOCKETS) //configure EventHubClient transport to AmqpWebSockets
				.checkpointStore(new BlobCheckpointStore(blobContainerAsyncClient));

		EventProcessorClient eventProcessorClient = eventProcessorClientBuilder.buildEventProcessorClient();

		// Create the Kafka Properties class to instantiate the Consumer with the desired settings:
		String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";";
		String jaasCfg = String.format(jaasTemplate, "kafka", "bpXalEi1iGwg");

		Kafka_Props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "52.163.204.3:9092");
		Kafka_Props.put(ProducerConfig.CLIENT_ID_CONFIG,"AZURE TEST");
		Kafka_Props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		Kafka_Props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		Kafka_Props.put(SaslConfigs.SASL_JAAS_CONFIG, jaasCfg);
		Kafka_Props.put(SaslConfigs.SASL_MECHANISM,"PLAIN");


		Kafka_Producer = new KafkaProducer<String, String>(Kafka_Props);


		System.out.println("Starting event processor");
		eventProcessorClient.start();

		System.out.println("Press enter to stop.");
		System.in.read();

		System.out.println("Stopping event processor");
		eventProcessorClient.stop();
		System.out.println("Event processor stopped.");

		System.out.println("Exiting process");
	}
}