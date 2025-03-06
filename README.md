# Competing Consumers Pattern Example

This project demonstrates the Competing Consumers pattern using a simple message queue and multiple consumers. The pattern is useful for processing messages concurrently to improve throughput and scalability.

## Project Structure

- `src/main/java/com/example/`
  - `Message.java`: Defines the `Message` class representing a message to be processed.
  - `MessageQueue.java`: Defines the `MessageQueue` class that manages the queue of messages.
  - `Consumer.java`: Defines the `Consumer` class that processes messages from the queue.
  - `MessageProcessor.java`: Defines the `MessageProcessor` class that processes messages in batches with resilience patterns.
  - `Main.java`: The main class that sets up the message queue, adds messages, starts multiple consumer threads, and initializes the metrics dashboard.
  - `visualization/MetricsDashboard.java`: Defines the `MetricsDashboard` class that provides real-time visualizations of the message processing metrics.

## How to Run

1. Ensure you have Java and Maven installed on your system.
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

## Output

The output will show each consumer processing messages, demonstrating the competing consumers pattern in action. The metrics dashboard will display real-time visualizations of the message processing metrics.
