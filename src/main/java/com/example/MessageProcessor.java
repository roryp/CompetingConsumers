package com.example;

import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Advanced message processor that implements processing pipeline with 
 * batching, circuit breaking, and other resilience patterns.
 */
public class MessageProcessor {
    private static final Logger logger = LoggerFactory.getLogger(MessageProcessor.class);
    private static final AtomicInteger PROCESSOR_COUNTER = new AtomicInteger(0);
    
    private final String name;
    private final MessageQueue messageQueue;
    private final int batchSize;
    private final int maxBatchSize;
    private final Duration maxBatchWait;
    
    // Thread-safe batch accumulator
    private final CopyOnWriteArrayList<Message> batch = new CopyOnWriteArrayList<>();
    
    // Circuit breaker for fault tolerance
    private final CircuitBreaker circuitBreaker;
    
    // Processing pipeline stages
    private final Function<Message, CompletableFuture<Message>> validator;
    private final Function<Message, CompletableFuture<Message>> transformer;
    private final Function<Message, CompletableFuture<Void>> consumer;
    
    // Metrics
    private final MeterRegistry registry = new SimpleMeterRegistry();
    private final Counter messagesProcessed;
    private final Counter messagesFailed;
    private final Timer processingTimer;
    private final Timer batchProcessingTimer;
    
    // Control flags
    private volatile boolean running = true;
    private final AtomicInteger batchSizeAdaptation = new AtomicInteger(0);
    
    /**
     * Create a new message processor with adaptive batch sizing
     */
    public MessageProcessor(MessageQueue messageQueue, String name, int initialBatchSize) {
        this.messageQueue = messageQueue;
        this.name = name + "-" + PROCESSOR_COUNTER.incrementAndGet();
        this.batchSize = initialBatchSize;
        this.maxBatchSize = initialBatchSize * 3;
        this.maxBatchWait = Duration.ofMillis(500);
        
        // Setup metrics
        this.messagesProcessed = registry.counter("processor." + this.name + ".processed");
        this.messagesFailed = registry.counter("processor." + this.name + ".failed");
        this.processingTimer = registry.timer("processor." + this.name + ".processing.time");
        this.batchProcessingTimer = registry.timer("processor." + this.name + ".batch.time");
        
        // Configure circuit breaker for fault tolerance
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
            .failureRateThreshold(50)
            .waitDurationInOpenState(Duration.ofMillis(1000))
            .permittedNumberOfCallsInHalfOpenState(5)
            .slidingWindowSize(10)
            .build();
            
        this.circuitBreaker = CircuitBreaker.of(this.name, circuitBreakerConfig);
        
        // Default pipeline stages
        this.validator = this::validateMessage;
        this.transformer = this::transformMessage;
        this.consumer = this::consumeMessage;
        
        logger.info("Message processor {} initialized with batch size {}", this.name, this.batchSize);
    }
    
    /**
     * Start the processor in a virtual thread
     */
    public Thread start() {
        return Thread.ofVirtual().name("processor-" + name).start(this::run);
    }
    
    /**
     * Main processing loop
     */
    private void run() {
        logger.info("Message processor {} started", name);
        
        try {
            while (running) {
                try {
                    // Adaptive batch collection
                    collectBatch();
                    
                    if (!batch.isEmpty()) {
                        // Process the current batch
                        processBatch();
                        
                        // Clear the batch
                        batch.clear();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.warn("Message processor {} was interrupted", name);
                    break;
                } catch (Exception e) {
                    logger.error("Error in message processor {}", name, e);
                    messagesFailed.increment();
                    
                    // Brief pause on error
                    Thread.sleep(100);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            logger.info("Message processor {} stopped", name);
        }
    }
    
    /**
     * Collect a batch of messages with adaptive sizing
     */
    private void collectBatch() throws InterruptedException {
        // Calculate current effective batch size based on performance
        int effectiveBatchSize = calculateAdaptiveBatchSize();
        
        // Use batch timer to measure collection time
        batchProcessingTimer.record(() -> {
            try {
                // First message with timeout
                Message firstMessage = messageQueue.getMessage(maxBatchWait);
                if (firstMessage != null) {
                    // Check if it's a poison message
                    if (firstMessage instanceof Message.Poison) {
                        logger.info("Processor {} received poison message, shutting down", name);
                        running = false;
                        batch.add(firstMessage);
                        return;
                    }
                    
                    // Add to batch
                    batch.add(firstMessage);
                    
                    // Try to get more messages up to batch size without waiting too long
                    for (int i = 1; i < effectiveBatchSize; i++) {
                        Message message = messageQueue.getMessage(Duration.ofMillis(10));
                        if (message == null) {
                            break;
                        }
                        
                        if (message instanceof Message.Poison) {
                            logger.info("Processor {} received poison message, shutting down", name);
                            running = false;
                            batch.add(message);
                            break;
                        }
                        
                        batch.add(message);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        });
        
        if (!batch.isEmpty()) {
            logger.debug("Collected batch of {} messages", batch.size());
        }
    }
    
    /**
     * Process a batch of messages through the pipeline
     */
    private void processBatch() {
        logger.info("Processing batch of {} messages", batch.size());
        
        // Process messages in parallel
        CompletableFuture<?>[] futures = batch.stream()
            .map(message -> processMessageWithCircuitBreaker(message))
            .toArray(CompletableFuture<?>[]::new);
        
        // Wait for all processing to complete
        CompletableFuture.allOf(futures).join();
        
        // Adapt batch size based on performance
        adjustBatchSize(batch.size());
        
        logger.info("Completed batch processing of {} messages", batch.size());
    }
    
    /**
     * Process a single message through the entire pipeline with circuit breaker
     */
    private CompletableFuture<Void> processMessageWithCircuitBreaker(Message message) {
        return processingTimer.record(() -> {
            try {
                // Use circuit breaker to protect against failures
                Supplier<CompletableFuture<Void>> decoratedSupplier = CircuitBreaker.decorateSupplier(
                    circuitBreaker,
                    () -> processMessageThroughPipeline(message)
                );
                
                return decoratedSupplier.get();
            } catch (Exception e) {
                logger.error("Error processing message: {}", message.getId(), e);
                messagesFailed.increment();
                return CompletableFuture.completedFuture(null);
            }
        });
    }
    
    /**
     * Process a message through all pipeline stages
     */
    private CompletableFuture<Void> processMessageThroughPipeline(Message message) {
        return validator.apply(message)
            .thenCompose(transformer)
            .thenCompose(consumer)
            .exceptionally(ex -> {
                logger.error("Pipeline processing error for message: {}", message.getId(), ex);
                messagesFailed.increment();
                return null;
            });
    }
    
    /**
     * Calculate an adaptive batch size based on recent performance
     */
    private int calculateAdaptiveBatchSize() {
        int adaptationValue = batchSizeAdaptation.get();
        
        // Simple adaptive algorithm - adjust batch size within bounds
        int calculatedBatchSize = batchSize + adaptationValue;
        
        // Keep within bounds
        if (calculatedBatchSize < 1) {
            return 1;
        } else if (calculatedBatchSize > maxBatchSize) {
            return maxBatchSize;
        }
        
        return calculatedBatchSize;
    }
    
    /**
     * Adjust the batch size based on processing results
     */
    private void adjustBatchSize(int lastBatchSize) {
        double avgProcessingTime = processingTimer.mean(TimeUnit.MILLISECONDS);
        
        // If processing is fast, try to increase batch size
        if (avgProcessingTime < 150 && lastBatchSize >= batchSize) {
            batchSizeAdaptation.incrementAndGet();
        } 
        // If processing is slow, reduce batch size
        else if (avgProcessingTime > 300) {
            batchSizeAdaptation.decrementAndGet();
        }
    }
    
    // Pipeline stage 1: Validate message
    private CompletableFuture<Message> validateMessage(Message message) {
        return CompletableFuture.supplyAsync(() -> {
            // In a real app, this would validate message content
            logger.debug("Validating message: {}", message.getId());
            return message;
        });
    }
    
    // Pipeline stage 2: Transform message (no actual transformation in demo)
    private CompletableFuture<Message> transformMessage(Message message) {
        return CompletableFuture.supplyAsync(() -> {
            // In a real app, this would transform the message
            logger.debug("Transforming message: {}", message.getId());
            return message;
        });
    }
    
    // Pipeline stage 3: Consume message
    private CompletableFuture<Void> consumeMessage(Message message) {
        return CompletableFuture.runAsync(() -> {
            logger.info("Processor {} consuming message {}", name, message.getId());
            
            // Simulate processing work with variable time
            try {
                Thread.sleep(50 + ThreadLocalRandom.current().nextInt(200));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            
            // Use pattern matching to handle different message types
            String result = switch (message) {
                case Message.Priority p -> "Priority message consumed: " + p.getContent();
                case Message.Standard s -> "Standard message consumed: " + s.getContent();
                case Message.Poison p -> "Poison message detected: " + p.getContent();
            };
            
            logger.info("Processor {} result: {}", name, result);
            messagesProcessed.increment();
        });
    }
    
    /**
     * Get processor metrics
     */
    public ProcessorMetrics getMetrics() {
        return new ProcessorMetrics(
            name,
            messagesProcessed.count(),
            messagesFailed.count(),
            processingTimer.mean(TimeUnit.MILLISECONDS),
            batchProcessingTimer.mean(TimeUnit.MILLISECONDS),
            calculateAdaptiveBatchSize(),
            circuitBreaker.getState().name()
        );
    }
    
    /**
     * Stop the processor gracefully
     */
    public void stop() {
        running = false;
    }
    
    /**
     * Processor metrics record
     */
    public record ProcessorMetrics(
        String name,
        double messagesProcessed,
        double messagesFailed,
        double avgProcessingTimeMs,
        double avgBatchTimeMs,
        int currentBatchSize,
        String circuitBreakerState
    ) {}
}