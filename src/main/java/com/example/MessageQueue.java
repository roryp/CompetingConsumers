package com.example;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.Flow.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class MessageQueue {
    private static final Logger logger = LoggerFactory.getLogger(MessageQueue.class);
    
    // Priority queue implementation with comparator ordering by message type
    private final PriorityBlockingQueue<Message> queue = new PriorityBlockingQueue<>(
        100,
        (m1, m2) -> m1.getType().compareTo(m2.getType())
    );
    
    // For reactive streams implementation
    private final SubmissionPublisher<Message> publisher = new SubmissionPublisher<>(
        ForkJoinPool.commonPool(), 
        Flow.defaultBufferSize(),
        (subscriber, throwable) -> logger.error("Error in subscriber", throwable)
    );
    
    // Metrics collection
    private final MeterRegistry registry = new SimpleMeterRegistry();
    private final Counter messagesReceived = registry.counter("messages.received");
    private final Counter messagesProcessed = registry.counter("messages.processed");
    private final Timer messageProcessingTime = registry.timer("message.processing.time");
    
    // Concurrency control
    private final ReentrantLock lock = new ReentrantLock();
    private final AtomicLong messageCount = new AtomicLong(0);
    private final Duration timeout = Duration.ofSeconds(5);
    
    /**
     * Add a message to the queue
     */
    public void addMessage(Message message) {
        lock.lock();
        try {
            queue.add(message);
            publisher.submit(message);
            messagesReceived.increment();
            messageCount.incrementAndGet();
            logger.info("Added message: {} of type {}", message.getId(), message.getType());
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * Get a message from the queue with priority handling.
     * Priority messages are processed first, then standard messages.
     * Blocks until a message is available or interrupted.
     */
    public Message getMessage() throws InterruptedException {
        Message message = queue.take();
        messagesProcessed.increment();
        messageCount.decrementAndGet();
        return message;
    }
    
    /**
     * Attempt to get a message with timeout
     */
    public Message getMessage(Duration timeout) throws InterruptedException {
        Message message = queue.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
        if (message != null) {
            messagesProcessed.increment();
            messageCount.decrementAndGet();
        }
        return message;
    }
    
    /**
     * Reactive streams subscription method
     */
    public void subscribe(Subscriber<? super Message> subscriber) {
        publisher.subscribe(subscriber);
    }
    
    /**
     * Get current metrics snapshot
     */
    public MessageQueueMetrics getMetrics() {
        return new MessageQueueMetrics(
            messagesReceived.count(),
            messagesProcessed.count(),
            messageCount.get(),
            messageProcessingTime.mean(TimeUnit.MILLISECONDS)
        );
    }
    
    /**
     * Check if queue is empty
     */
    public boolean isEmpty() {
        return queue.isEmpty();
    }
    
    /**
     * Current queue size
     */
    public int size() {
        return queue.size();
    }
    
    /**
     * Metrics record
     */
    public record MessageQueueMetrics(
        double messagesReceived,
        double messagesProcessed,
        long currentQueueSize,
        double averageProcessingTimeMs
    ) {}
}
