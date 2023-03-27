package com.prime;

import com.azure.messaging.eventhubs.*;
import java.util.Arrays;
import java.util.List;

public class Sender {
    private static final String connectionString = "sb://pr100-test-booking-th24i9.servicebus.windows.net/;SharedAccessKeyName=pr100-test-vendors-listen;SharedAccessKey=oD7tyELIqLZI6UPp4AaD/OSviQ7Bwz10/1s321IonFU=";
    private static final String eventHubName = "VENDOR";

    public static void main(String[] args) {
        publishEvents();
    }
    
    public static void publishEvents() {
        // create a producer client
        EventHubProducerClient producer = new EventHubClientBuilder()
            .connectionString(connectionString, eventHubName)
            .buildProducerClient();

        // sample events in an array
        List<EventData> allEvents = Arrays.asList(new EventData("First test message from Mule"), new EventData("Second test message from Mule"));

        // create a batch
        EventDataBatch eventDataBatch = producer.createBatch();

        for (EventData eventData : allEvents) {
            // try to add the event from the array to the batch
            if (!eventDataBatch.tryAdd(eventData)) {
                // if the batch is full, send it and then create a new batch
            	System.out.println("sending a message to Hub");
                producer.send(eventDataBatch);
                System.out.println("Successfully sent");
                eventDataBatch = producer.createBatch();

                // Try to add that event that couldn't fit before.
                if (!eventDataBatch.tryAdd(eventData)) {
                    throw new IllegalArgumentException("Event is too large for an empty batch. Max size: "
                        + eventDataBatch.getMaxSizeInBytes());
                }
            }
        }
        // send the last batch of remaining events
        if (eventDataBatch.getCount() > 0) {
            producer.send(eventDataBatch);
        }
        producer.close();
    }
}
