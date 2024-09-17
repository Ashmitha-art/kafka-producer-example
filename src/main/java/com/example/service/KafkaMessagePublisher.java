package com.example.service;

import java.util.concurrent.CompletableFuture;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.example.dto.Customer;

import org.springframework.kafka.support.SendResult;

@Service
public class KafkaMessagePublisher {

    @Autowired
    private KafkaTemplate<String, Object> template;

    public void sendMessageToTopic(String message) {
        // Send the message to Kafka topic asynchronously
        CompletableFuture<SendResult<String, Object>> future = template.send("customer-example", message);

        // Handle the result of the send operation
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                // Success case: print the offset
                System.out.println("Sent message = [" + message + "] with offset=[" +
                        result.getRecordMetadata().offset() + "]");
            } else {
                // Error case: print the exception and full error message
                System.out.println("Unable to send message = [" + message + "] Due to: " + ex.getMessage());
                ex.printStackTrace(); // Optional: print the stack trace for debugging
            }
        });
    }

    public void sendEventsToTopic(Customer customer) {
        // Send the message to Kafka topic asynchronously
        try {
            CompletableFuture<SendResult<String, Object>> future = template.send("customer-example", customer);

            // Handle the result of the send operation
            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    // Success case: print the offset
                    System.out.println("Sent message = [" + customer.toString() + "] with offset=[" +
                            result.getRecordMetadata().offset() + "]");
                } else {
                    // Error case: print the exception and full error message
                    System.out.println("Unable to send message = [" + customer + "] Due to: " + ex.getMessage());
                    ex.printStackTrace(); // Optional: print the stack trace for debugging
                }
            });
        } catch (Exception ex) {
            System.out.println("Error" + ex.getMessage());
        }
    }
}
