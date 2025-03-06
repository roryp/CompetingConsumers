package com.example;

import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class Consumer implements Flow.Subscriber<Message> {
    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);
    private static final Random RANDOM = new Random();
    private static final AtomicInteger CONSUMER_COUNTER = new AtomicInteger(0);
    
    private final String name;
    private final MessageQueue messageQueue;
    private final int processingTimeMs;
    private Flow.Subscription subscription;
    private volatile boolean running = true;
    
    // Resilience bulkhead to limit concurrent executions
    private final Bulkhead bulkhead;
    
    // Processing function that can be customized
    private final Function<Message, CompletableFuture<Void>> processor;
    
    public Consumer(MessageQueue messageQueue, String name) {
        this(messageQueue, name, 100); // Default processing time
    }
    
    public Consumer(MessageQueue messageQueue, String name, int processingTimeMs) {
        this.messageQueue = messageQueue;
        this.name = name;
        this.processingTimeMs = processingTimeMs;
        
        // Create a bulkhead to limit concurrent message processing
        BulkheadConfig bulkheadConfig = BulkheadConfig.custom()
            .maxConcurrentCalls(2)
            .maxWaitDuration(Duration.ofMillis(500))
            .build();
        this.bulkhead = Bulkhead.of(name, bulkheadConfig);
        
        // Default processor implementation
        this.processor = this::processMessage;
        
        logger.info("Consumer {} initialized", name);
    }
    
    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        this.subscription = subscription;
        // Request messages one at a time (for demonstration)
        subscription.request(1);
        logger.info("Consumer {} subscribed to message queue", name);
    }
    
    @Override
    public void onNext(Message message) {
        logger.info("Consumer {} received message: {}", name, message.getId());
        
        // Handle message based on type using pattern matching
        if (message instanceof Message.Poison) {
            logger.info("Consumer {} received poison message, shutting down", name);
            running = false;
            subscription.cancel();
            return;
        }
        
        // Process the message with bulkhead protection
        try {
            Bulkhead.decorateSupplier(bulkhead, () -> {
                // Process message
                CompletableFuture<Void> future = processor.apply(message);
                try {
                    future.get(5, TimeUnit.SECONDS); // Wait with timeout
                } catch (Exception e) {
                    logger.error("Error processing message {}", message.getId(), e);
                }
                return null;
            }).get();
        } catch (Exception e) {
            logger.error("Bulkhead rejected processing for message {}", message.getId(), e);
        }
        
        // Request one more message if still running
        if (running) {
            subscription.request(1);
        }
    }
    
    @Override
    public void onError(Throwable t) {
        logger.error("Consumer {} encountered an error", name, t);
    }
    
    @Override
    public void onComplete() {
        logger.info("Consumer {} completed subscription", name);
    }
    
    /**
     * Legacy method to run as a Runnable (polling mode instead of reactive)
     */
    public void run() {
        logger.info("Consumer {} started in polling mode", name);
        try {
            while (running) {
                Message message = messageQueue.getMessage();
                
                // Use instanceof pattern matching (Java 21 feature)
                if (message instanceof Message.Poison p) {
                    logger.info("Consumer {} received poison pill, shutting down: {}", 
                        name, p.getContent());
                    break;
                }
                
                // Process the message with bulkhead protection
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    try {
                        bulkhead.executeCheckedSupplier(() -> {
                            processMessage(message).join();
                            return null;
                        });
                    } catch (Throwable e) {
                        logger.error("Error processing message in polling mode", e);
                    }
                });
            }
        } catch (InterruptedException e) {
            logger.warn("Consumer {} was interrupted", name);
            Thread.currentThread().interrupt();
        }
        logger.info("Consumer {} stopped", name);
    }
    
    /**
     * Process a message with simulated work
     */
    private CompletableFuture<Void> processMessage(Message message) {
        return CompletableFuture.runAsync(() -> {
            try {
                // Simulate variable processing time with jitter
                int processingTime = processingTimeMs + RANDOM.nextInt(processingTimeMs / 2);
                logger.info("Consumer {} processing message {} (type {}) for {}ms", 
                    name, message.getId(), message.getType(), processingTime);
                Thread.sleep(processingTime);
                
                // Use switch expression with pattern matching (Java 21)
                String result = switch (message) {
                    case Message.Priority p -> "Priority message processed: " + p.getContent();
                    case Message.Standard s -> "Standard message processed: " + s.getContent();
                    case Message.Poison p -> "Poison message detected: " + p.getContent();
                };
                
                logger.info("Consumer {} finished processing message {}: {}", 
                    name, message.getId(), result);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Processing interrupted for message {}", message.getId());
            }
        });
    }
    
    /**
     * Stop the consumer gracefully
     */
    public void stop() {
        running = false;
        if (subscription != null) {
            subscription.cancel();
        }
    }
}
