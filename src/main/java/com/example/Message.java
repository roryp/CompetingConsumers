package com.example;

import java.time.Instant;
import java.util.UUID;

public sealed interface Message permits Message.Standard, Message.Priority, Message.Poison {
    String getId();
    String getContent();
    Instant getCreatedAt();
    MessageType getType();
    
    enum MessageType {
        STANDARD, PRIORITY, POISON
    }
    
    // Pattern for creating messages
    static Message of(String content, MessageType type) {
        return switch(type) {
            case PRIORITY -> new Priority(UUID.randomUUID().toString(), content, Instant.now());
            case POISON -> new Poison(UUID.randomUUID().toString(), content, Instant.now());
            case STANDARD -> new Standard(UUID.randomUUID().toString(), content, Instant.now());
        };
    }
    
    // Standard message with normal priority
    record Standard(String id, String content, Instant createdAt) implements Message {
        @Override
        public String getId() { return id; }
        
        @Override
        public String getContent() { return content; }
        
        @Override
        public Instant getCreatedAt() { return createdAt; }
        
        @Override
        public MessageType getType() { return MessageType.STANDARD; }
    }
    
    // High-priority message that should be processed first
    record Priority(String id, String content, Instant createdAt) implements Message {
        @Override
        public String getId() { return id; }
        
        @Override
        public String getContent() { return content; }
        
        @Override
        public Instant getCreatedAt() { return createdAt; }
        
        @Override
        public MessageType getType() { return MessageType.PRIORITY; }
    }
    
    // Poison message that signals consumers to terminate
    record Poison(String id, String content, Instant createdAt) implements Message {
        @Override
        public String getId() { return id; }
        
        @Override
        public String getContent() { return content; }
        
        @Override
        public Instant getCreatedAt() { return createdAt; }
        
        @Override
        public MessageType getType() { return MessageType.POISON; }
    }
}
