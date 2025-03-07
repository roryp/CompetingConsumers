package com.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.example.visualization.MetricsDashboard;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private static final Random RANDOM = new Random();
    
    public static void main(String[] args) throws Exception {
        // Create message queue
        MessageQueue messageQueue = new MessageQueue();
        
        // Track consumers for graceful shutdown
        List<Consumer> consumers = new ArrayList<>();
        List<MessageProcessor> processors = new ArrayList<>();
        
        // Message producer ID counter
        AtomicInteger producerId = new AtomicInteger(1);
        
        // Initialize and start the metrics dashboard
        MetricsDashboard dashboard = new MetricsDashboard(messageQueue, consumers, processors);
        
        // Demo using structured concurrency with virtual threads
        logger.info("Starting advanced competing consumer pattern demo");
        
        try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
            // PART 1: Traditional consumer approach
            logger.info("================ PART 1: Traditional Competing Consumer Pattern ================");
            
            // Start the message producer using virtual threads
            scope.fork(() -> {
                logger.info("Starting message producer");
                
                // Regular message production
                for (int i = 1; i <= 20; i++) {
                    // Create different types of messages with varying probabilities
                    Message.MessageType type = RANDOM.nextDouble() < 0.2 
                        ? Message.MessageType.PRIORITY 
                        : Message.MessageType.STANDARD;
                    
                    Message message = Message.of(
                        "Message " + producerId.getAndIncrement(), 
                        type
                    );
                    
                    messageQueue.addMessage(message);
                    
                    try {
                        // Random delay between message production
                        Thread.sleep(RANDOM.nextInt(100));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return null;
                    }
                }
                
                logger.info("Producer finished sending regular messages");
                return null;
            });
            
            // Consumer creation - demonstrate both approaches
            
            // Approach 1: Reactive consumers using Flow API
            logger.info("Creating reactive consumers");
            for (int i = 1; i <= 3; i++) {
                final int id = i;
                // Create consumer with randomized processing time to show varying performance
                Consumer consumer = new Consumer(messageQueue, "ReactiveConsumer-" + id, 
                    50 + RANDOM.nextInt(200));
                consumers.add(consumer);
                
                // Subscribe consumer to message queue
                messageQueue.subscribe(consumer);
            }
            
            // Approach 2: Traditional polling consumers using virtual threads
            logger.info("Creating polling consumers");
            for (int i = 1; i <= 3; i++) {
                final int id = i;
                Consumer consumer = new Consumer(messageQueue, "PollingConsumer-" + id,
                    50 + RANDOM.nextInt(200));
                consumers.add(consumer);
                
                // Use Project Loom virtual threads for lightweight concurrency
                scope.fork(() -> {
                    consumer.run();
                    return null;
                });
            }
            
            // Wait for initial processing for a few seconds
            Thread.sleep(3000);
            
            // Add some high-priority messages
            scope.fork(() -> {
                logger.info("Sending priority messages");
                for (int i = 1; i <= 5; i++) {
                    Message message = Message.of(
                        "URGENT Message " + producerId.getAndIncrement(), 
                        Message.MessageType.PRIORITY
                    );
                    messageQueue.addMessage(message);
                    Thread.sleep(500);
                }
                return null;
            });
            
            // Wait for processing to complete
            Thread.sleep(2000);
            
            // Print metrics for first part
            printMetrics(messageQueue);
            
            // PART 2: Advanced Batch Processing with Pipeline
            logger.info("\n================ PART 2: Advanced Batch Processing Pattern ================");
            
            // Create message processors with different batch sizes
            for (int i = 1; i <= 3; i++) {
                // Each processor has a different initial batch size
                MessageProcessor processor = new MessageProcessor(
                    messageQueue, 
                    "BatchProcessor", 
                    i * 2 // Different batch sizes: 2, 4, 6
                );
                processors.add(processor);
                
                // Start processor in virtual thread
                processor.start();
            }
            
            // Generate a burst of messages for batch processing
            scope.fork(() -> {
                logger.info("Starting high-volume message production for batch processing");
                
                for (int batch = 0; batch < 3; batch++) {
                    // Create 30 messages in rapid succession (90 total)
                    for (int i = 0; i < 30; i++) {
                        // Mix in some priority messages
                        Message.MessageType type = RANDOM.nextDouble() < 0.15
                            ? Message.MessageType.PRIORITY
                            : Message.MessageType.STANDARD;
                            
                        Message message = Message.of(
                            "Batch" + batch + "-Message-" + i, 
                            type
                        );
                        
                        messageQueue.addMessage(message);
                        
                        // Small delay to not overwhelm logging
                        if (i % 10 == 0) {
                            Thread.sleep(50);
                        }
                    }
                    
                    // Pause between batches
                    logger.info("Batch {} sent, pausing...", batch);
                    Thread.sleep(1000);
                }
                
                logger.info("Completed high-volume message production");
                return null;
            });
            
            // Wait for batch processing to work through the messages
            Thread.sleep(8000);
            
            // Print processor metrics
            printProcessorMetrics(processors);
            
            // Final metrics
            printMetrics(messageQueue);
            
            // Send poison messages to gracefully shut down all consumers
            sendPoisonMessages(messageQueue, consumers.size() + processors.size());
            
            // Wait for all processing to complete
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            logger.error("Demo interrupted", e);
            Thread.currentThread().interrupt();
        } finally {
            // Force stop any consumers that didn't shut down
            for (Consumer consumer : consumers) {
                consumer.stop();
            }
            
            for (MessageProcessor processor : processors) {
                processor.stop();
            }
            
            // Ensure the dashboard is properly shut down at the end
            dashboard.shutdown();
            logger.info("Demo completed");
        }
    }
    
    /**
     * Print message queue metrics
     */
    private static void printMetrics(MessageQueue messageQueue) {
        MessageQueue.MessageQueueMetrics metrics = messageQueue.getMetrics();
        logger.info("----- Message Queue Metrics -----");
        logger.info("Messages received: {}", metrics.messagesReceived());
        logger.info("Messages processed: {}", metrics.messagesProcessed());
        logger.info("Current queue size: {}", metrics.currentQueueSize());
        logger.info("Avg processing time: {} ms", metrics.averageProcessingTimeMs());
        logger.info("Avg wait time: {} ms", metrics.averageWaitTimeMs());
        logger.info("---------------------------------");
    }
    
    /**
     * Print processor metrics
     */
    private static void printProcessorMetrics(List<MessageProcessor> processors) {
        logger.info("----- Message Processor Metrics -----");
        for (MessageProcessor processor : processors) {
            MessageProcessor.ProcessorMetrics metrics = processor.getMetrics();
            logger.info("Processor: {} (batch size: {})", 
                metrics.name(), metrics.currentBatchSize());
            logger.info("  Messages processed: {}", metrics.messagesProcessed());
            logger.info("  Messages failed: {}", metrics.messagesFailed());
            logger.info("  High-priority messages processed: {}", metrics.highPriorityMessagesProcessed());
            logger.info("  Standard messages processed: {}", metrics.standardMessagesProcessed());
            logger.info("  Avg processing time: {} ms", metrics.avgProcessingTimeMs());
            logger.info("  Avg batch time: {} ms", metrics.avgBatchTimeMs());
            logger.info("  Avg wait time: {} ms", metrics.avgWaitTimeMs());
            logger.info("  Circuit breaker state: {}", metrics.circuitBreakerState());
            logger.info("---------------------------------");
        }
    }
    
    /**
     * Send poison messages to gracefully shut down consumers
     */
    private static void sendPoisonMessages(MessageQueue messageQueue, int consumerCount) {
        logger.info("Sending poison messages to shut down consumers");
        for (int i = 0; i < consumerCount; i++) {
            messageQueue.addMessage(Message.of(
                "Shutdown signal", 
                Message.MessageType.POISON
            ));
        }
    }
    
    /**
     * Set up real-time alerts based on collected metrics
     */
    private static void setupRealTimeAlerts(MeterRegistry registry) {
        Counter highPriorityMessagesProcessed = registry.counter("messages.processed.high_priority");
        Counter standardMessagesProcessed = registry.counter("messages.processed.standard");
        Timer messageWaitTime = registry.timer("message.wait.time");

        // Example alert: High-priority messages processed exceeds threshold
        if (highPriorityMessagesProcessed.count() > 100) {
            logger.warn("High-priority messages processed exceeded threshold: {}", highPriorityMessagesProcessed.count());
        }

        // Example alert: Standard messages processed exceeds threshold
        if (standardMessagesProcessed.count() > 500) {
            logger.warn("Standard messages processed exceeded threshold: {}", standardMessagesProcessed.count());
        }

        // Example alert: Message wait time exceeds threshold
        if (messageWaitTime.mean(TimeUnit.MILLISECONDS) > 200) {
            logger.warn("Average message wait time exceeded threshold: {} ms", messageWaitTime.mean(TimeUnit.MILLISECONDS));
        }
    }
}
