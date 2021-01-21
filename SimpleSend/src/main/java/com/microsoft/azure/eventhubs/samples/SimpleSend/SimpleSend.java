package com.microsoft.azure.eventhubs.samples.SimpleSend;

import com.azure.messaging.eventhubs.*;
//import util.properties packages
import java.util.Properties;

public class SimpleSend {
    public static void main(String[] args) {
        final String connectionString = "Endpoint=sb://ehubns-kafkapoc-zaky.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=8IEM/bAExPyMKi1ItJA+SIe+UAZpbcO5B5Jz5ztCtW4=";
        final String eventHubName = "ehub-kafkapoc-zaky";

        // create a producer using the namespace connection string and event hub name
        EventHubProducerClient producer = new EventHubClientBuilder()
                .connectionString(connectionString, eventHubName)
                .buildProducerClient();

        // prepare a batch of events to send to the event hub
        EventDataBatch batch = producer.createBatch();
        batch.tryAdd(new EventData("First event"));
        batch.tryAdd(new EventData("Second event"));
        batch.tryAdd(new EventData("Third event"));
        batch.tryAdd(new EventData("Fourth event"));
        batch.tryAdd(new EventData("Fifth event"));

        // send the batch of events to the event hub
        producer.send(batch);

        // close the producer
        producer.close();
    }
}