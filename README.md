# Competing Consumers Pattern Example

This project demonstrates the Competing Consumers pattern using a simple message queue and multiple consumers. The pattern is useful for processing messages concurrently to improve throughput and scalability.

## Project Structure

- `src/main/java/com/example/`
  - `Message.java`: Defines the `Message` interface with three implementations: `Standard`, `Priority`, and `Poison` messages.
  - `MessageQueue.java`: Implements a priority-based message queue with metrics collection and both reactive and polling consumer support.
  - `Consumer.java`: Defines the `Consumer` class that processes messages from the queue with resilience patterns.
  - `MessageProcessor.java`: Implements batch processing of messages with circuit breakers, adaptive batch sizing, and metrics collection.
  - `Main.java`: The main class that sets up the message queue, adds messages, starts multiple consumer threads, and initializes the metrics dashboard.
  - `visualization/MetricsDashboard.java`: Provides real-time visualizations of message processing metrics through interactive charts.

## Features

- **Multiple message types**: Support for standard, high-priority, and poison messages
- **Dual consumption models**: Both reactive (using Java's Flow API) and polling consumer options
- **Resilience patterns**: Circuit breakers, bulkheads, and timeout handling
- **Batch processing**: Adaptive batch sizing based on system performance
- **Virtual threads**: Uses Project Loom's virtual threads for efficient concurrency
- **Real-time metrics**: Comprehensive metrics collection and visualization
- **Interactive dashboard**: Real-time charts with customizable refresh rates

## How to Run

1. Ensure you have Java 21 or newer and Maven installed on your system.
2. Navigate to the project directory.
3. Run the following command to compile and execute the project:

```sh
mvn clean compile exec:java -Dexec.mainClass=com.example.Main
```

## How It Works

1. The `Main` class creates an instance of `MessageQueue`.
2. It adds 20 messages to the queue.
3. It starts 3 reactive consumer threads and 3 polling consumer threads, each running an instance of the `Consumer` class.
4. It initializes and starts the `MetricsDashboard` to display real-time visualizations of the message processing metrics.
5. It adds some high-priority messages to the queue.
6. It creates 3 message processors with different batch sizes and starts them in virtual threads.
7. It generates a burst of messages for batch processing.
8. It prints the metrics for the message queue and processors.
9. It sends poison messages to gracefully shut down all consumers and processors.

## Metrics Dashboard

The `MetricsDashboard` provides a comprehensive visualization of the system's performance metrics:

- **Queue Size Chart**: Displays the current queue size over time
- **Processing Rate Chart**: Shows the rate of message reception vs. processing
- **Consumer Comparison Chart**: Compares performance across different consumers
- **Processor Comparison Chart**: Displays processing statistics for each processor
- **Circuit Breaker State Chart**: Visualizes the state of circuit breakers for fault tolerance
- **Message Type Chart**: Breaks down message processing by type (standard vs. priority)
- **Message Wait Time Chart**: Shows the average time messages spend waiting in the queue

### Dashboard Controls

The dashboard includes interactive controls:
- **Pause/Resume**: Toggle the automatic refresh of the dashboard
- **Refresh Rate**: Adjust how frequently the charts update (0.5s, 1s, 2s, or 5s)

## Advanced Features

### Adaptive Batch Processing

The `MessageProcessor` class implements adaptive batch sizing that adjusts based on system performance. If processing is fast, batch sizes increase automatically; if processing slows down, batch sizes decrease.

### Resilience Patterns

This project demonstrates several resilience patterns:
- **Circuit breakers**: Prevent cascading failures when systems are under stress
- **Bulkheads**: Isolate components to contain failures
- **Timeout handling**: Ensure resources aren't tied up indefinitely
- **Graceful degradation**: System continues functioning even when parts fail

### Message Prioritization

The queue automatically prioritizes high-priority messages, ensuring critical messages are processed first.

## Output

The application produces both console output showing each consumer processing messages and a graphical metrics dashboard that displays real-time visualizations of the system's performance.
