package com.example.visualization;

import com.example.Consumer;
import com.example.MessageProcessor;
import com.example.MessageQueue;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.category.BarRenderer;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.time.Millisecond;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.*;
import java.awt.*;
import java.util.List;
import java.util.concurrent.*;

public class MetricsDashboard {
    private static final Logger logger = LoggerFactory.getLogger(MetricsDashboard.class);
    
    private final JFrame frame;
    private final MessageQueue messageQueue;
    private final List<Consumer> consumers;
    private final List<MessageProcessor> processors;
    
    // Charts
    private JFreeChart queueSizeChart;
    private JFreeChart processingRateChart;
    private JFreeChart consumerComparisonChart;
    private JFreeChart processorComparisonChart;
    private JFreeChart circuitBreakerStateChart;
    private JFreeChart messageTypeChart;
    private JFreeChart messageWaitTimeChart;
    
    // Datasets
    private final TimeSeries queueSizeSeries;
    private final TimeSeries messagesReceivedSeries;
    private final TimeSeries messagesProcessedSeries;
    private final DefaultCategoryDataset consumerDataset;
    private final DefaultCategoryDataset processorDataset;
    private final DefaultCategoryDataset circuitBreakerDataset;
    private final DefaultCategoryDataset messageTypeDataset;
    private final TimeSeries messageWaitTimeSeries;
    
    // Dashboard update thread
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private ScheduledFuture<?> updateTask;
    
    public MetricsDashboard(MessageQueue messageQueue, List<Consumer> consumers, List<MessageProcessor> processors) {
        this.messageQueue = messageQueue;
        this.consumers = consumers;
        this.processors = processors;
        
        // Initialize datasets
        this.queueSizeSeries = new TimeSeries("Queue Size");
        this.messagesReceivedSeries = new TimeSeries("Messages Received");
        this.messagesProcessedSeries = new TimeSeries("Messages Processed");
        this.consumerDataset = new DefaultCategoryDataset();
        this.processorDataset = new DefaultCategoryDataset();
        this.circuitBreakerDataset = new DefaultCategoryDataset();
        this.messageTypeDataset = new DefaultCategoryDataset();
        this.messageWaitTimeSeries = new TimeSeries("Message Wait Time");
        
        // Create UI
        frame = new JFrame("Competing Consumer Pattern Metrics Dashboard");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setSize(1200, 800);
        frame.setLayout(new GridLayout(4, 2));
        
        // Initialize charts
        initializeCharts();
        
        // Add chart panels to frame
        frame.add(createChartPanel(queueSizeChart));
        frame.add(createChartPanel(processingRateChart));
        frame.add(createChartPanel(consumerComparisonChart));
        frame.add(createChartPanel(processorComparisonChart));
        frame.add(createChartPanel(circuitBreakerStateChart));
        frame.add(createChartPanel(messageTypeChart));
        frame.add(createChartPanel(messageWaitTimeChart));
        
        // Add control panel
        frame.add(createControlPanel());
        
        // Make visible
        frame.setVisible(true);
        
        // Start update task
        startUpdating();
    }
    
    private void initializeCharts() {
        // Queue size time series chart
        TimeSeriesCollection queueDataset = new TimeSeriesCollection();
        queueDataset.addSeries(queueSizeSeries);
        queueSizeChart = ChartFactory.createTimeSeriesChart(
            "Queue Size Over Time", 
            "Time", 
            "Number of Messages",
            queueDataset,
            true,
            true,
            false
        );
        
        // Message processing rate chart
        TimeSeriesCollection rateDataset = new TimeSeriesCollection();
        rateDataset.addSeries(messagesReceivedSeries);
        rateDataset.addSeries(messagesProcessedSeries);
        processingRateChart = ChartFactory.createTimeSeriesChart(
            "Message Processing Rate",
            "Time",
            "Messages",
            rateDataset,
            true,
            true,
            false
        );
        
        // Consumer comparison chart
        consumerComparisonChart = ChartFactory.createBarChart(
            "Consumer Performance",
            "Consumer",
            "Messages Processed",
            consumerDataset,
            PlotOrientation.VERTICAL,
            true,
            true,
            false
        );
        
        // Processor comparison chart
        processorComparisonChart = ChartFactory.createBarChart(
            "Processor Performance",
            "Processor",
            "Messages Processed",
            processorDataset,
            PlotOrientation.VERTICAL,
            true,
            true,
            false
        );
        
        // Circuit breaker state chart
        circuitBreakerStateChart = ChartFactory.createBarChart(
            "Circuit Breaker States",
            "Processor",
            "State",
            circuitBreakerDataset,
            PlotOrientation.VERTICAL,
            true,
            true,
            false
        );
        
        // Message type chart
        messageTypeChart = ChartFactory.createBarChart(
            "Message Types Processed",
            "Message Type",
            "Count",
            messageTypeDataset,
            PlotOrientation.VERTICAL,
            true,
            true,
            false
        );
        
        // Message wait time chart
        TimeSeriesCollection waitTimeDataset = new TimeSeriesCollection();
        waitTimeDataset.addSeries(messageWaitTimeSeries);
        messageWaitTimeChart = ChartFactory.createTimeSeriesChart(
            "Message Wait Time",
            "Time",
            "Wait Time (ms)",
            waitTimeDataset,
            true,
            true,
            false
        );
        
        // Customize charts
        customizeCharts();
    }
    
    private void customizeCharts() {
        // Customize queue size chart colors
        queueSizeChart.getPlot().setBackgroundPaint(Color.WHITE);
        
        // Customize processing rate chart
        processingRateChart.getPlot().setBackgroundPaint(Color.WHITE);
        
        // Customize consumer comparison chart
        CategoryPlot consumerPlot = consumerComparisonChart.getCategoryPlot();
        consumerPlot.setBackgroundPaint(Color.WHITE);
        BarRenderer consumerRenderer = (BarRenderer) consumerPlot.getRenderer();
        consumerRenderer.setSeriesPaint(0, new Color(79, 129, 189)); // Blue bars
        
        // Customize processor comparison chart
        CategoryPlot processorPlot = processorComparisonChart.getCategoryPlot();
        processorPlot.setBackgroundPaint(Color.WHITE);
        BarRenderer processorRenderer = (BarRenderer) processorPlot.getRenderer();
        processorRenderer.setSeriesPaint(0, new Color(155, 187, 89)); // Green bars
        processorRenderer.setSeriesPaint(1, new Color(192, 80, 77));  // Red bars for failures
        
        // Customize circuit breaker chart
        CategoryPlot circuitPlot = circuitBreakerStateChart.getCategoryPlot();
        circuitPlot.setBackgroundPaint(Color.WHITE);
        NumberAxis rangeAxis = (NumberAxis) circuitPlot.getRangeAxis();
        rangeAxis.setVisible(false); // Hide the numeric axis as we're just showing states
        
        // Customize message type chart
        CategoryPlot messageTypePlot = messageTypeChart.getCategoryPlot();
        messageTypePlot.setBackgroundPaint(Color.WHITE);
        BarRenderer messageTypeRenderer = (BarRenderer) messageTypePlot.getRenderer();
        messageTypeRenderer.setSeriesPaint(0, new Color(79, 129, 189)); // Blue bars for high-priority
        messageTypeRenderer.setSeriesPaint(1, new Color(155, 187, 89)); // Green bars for standard
        
        // Customize message wait time chart
        messageWaitTimeChart.getPlot().setBackgroundPaint(Color.WHITE);
    }
    
    private ChartPanel createChartPanel(JFreeChart chart) {
        ChartPanel panel = new ChartPanel(chart);
        panel.setPreferredSize(new Dimension(400, 300));
        panel.setBorder(BorderFactory.createEmptyBorder(15, 15, 15, 15));
        panel.setBackground(Color.WHITE);
        return panel;
    }
    
    private JPanel createControlPanel() {
        JPanel panel = new JPanel();
        panel.setLayout(new FlowLayout());
        panel.setBorder(BorderFactory.createTitledBorder("Dashboard Controls"));
        
        // Add pause/resume button
        JButton pauseButton = new JButton("Pause Updates");
        pauseButton.addActionListener(e -> {
            if (updateTask != null && !updateTask.isCancelled()) {
                updateTask.cancel(false);
                pauseButton.setText("Resume Updates");
            } else {
                startUpdating();
                pauseButton.setText("Pause Updates");
            }
        });
        panel.add(pauseButton);
        
        // Add refresh rate control
        JComboBox<String> refreshRateComboBox = new JComboBox<>(new String[]{"0.5s", "1s", "2s", "5s"});
        refreshRateComboBox.setSelectedIndex(1); // Default to 1s
        refreshRateComboBox.addActionListener(e -> {
            if (updateTask != null) {
                updateTask.cancel(false);
            }
            
            String selectedRate = (String) refreshRateComboBox.getSelectedItem();
            long rateInMs = switch (selectedRate) {
                case "0.5s" -> 500;
                case "2s" -> 2000;
                case "5s" -> 5000;
                default -> 1000; // Default to 1s
            };
            
            startUpdating(rateInMs);
        });
        
        panel.add(new JLabel("Refresh Rate: "));
        panel.add(refreshRateComboBox);
        
        return panel;
    }
    
    private void startUpdating() {
        startUpdating(1000); // Default to 1 second update interval
    }
    
    private void startUpdating(long intervalMs) {
        updateTask = scheduler.scheduleAtFixedRate(this::updateCharts, 0, intervalMs, TimeUnit.MILLISECONDS);
    }
    
    private void updateCharts() {
        try {
            SwingUtilities.invokeLater(() -> {
                updateQueueSizeChart();
                updateProcessingRateChart();
                updateConsumerComparisonChart();
                updateProcessorComparisonChart();
                updateCircuitBreakerStateChart();
                updateMessageTypeChart();
                updateMessageWaitTimeChart();
            });
        } catch (Exception e) {
            logger.error("Error updating charts", e);
        }
    }
    
    private void updateQueueSizeChart() {
        MessageQueue.MessageQueueMetrics metrics = messageQueue.getMetrics();
        Millisecond now = new Millisecond();
        queueSizeSeries.addOrUpdate(now, metrics.currentQueueSize());
    }
    
    private void updateProcessingRateChart() {
        MessageQueue.MessageQueueMetrics metrics = messageQueue.getMetrics();
        Millisecond now = new Millisecond();
        messagesReceivedSeries.addOrUpdate(now, metrics.messagesReceived());
        messagesProcessedSeries.addOrUpdate(now, metrics.messagesProcessed());
    }
    
    private void updateConsumerComparisonChart() {
        consumerDataset.clear();
        
        // In a real implementation, you would collect metrics from consumers
        // For this demo, we'll use mock data based on consumer names
        for (int i = 0; i < consumers.size(); i++) {
            Consumer consumer = consumers.get(i);
            // Use the index as a stand-in for processed message count 
            // (would be actual metrics in real implementation)
            consumerDataset.addValue(i + 5, "Messages Processed", consumer.toString());
        }
    }
    
    private void updateProcessorComparisonChart() {
        processorDataset.clear();
        
        // Get actual metrics from processors
        for (MessageProcessor processor : processors) {
            MessageProcessor.ProcessorMetrics metrics = processor.getMetrics();
            processorDataset.addValue(metrics.messagesProcessed(), "Messages Processed", metrics.name());
            processorDataset.addValue(metrics.messagesFailed(), "Messages Failed", metrics.name());
        }
    }
    
    private void updateCircuitBreakerStateChart() {
        circuitBreakerDataset.clear();
        
        // Get circuit breaker states from processors
        for (MessageProcessor processor : processors) {
            MessageProcessor.ProcessorMetrics metrics = processor.getMetrics();
            // Convert state to numeric value for visualization
            double stateValue = switch (metrics.circuitBreakerState()) {
                case "CLOSED" -> 1.0;
                case "OPEN" -> 2.0;
                case "HALF_OPEN" -> 1.5;
                default -> 0.0;
            };
            circuitBreakerDataset.addValue(stateValue, "Circuit Breaker State", metrics.name());
        }
    }
    
    private void updateMessageTypeChart() {
        messageTypeDataset.clear();
        
        // Get message type counts from processors
        for (MessageProcessor processor : processors) {
            MessageProcessor.ProcessorMetrics metrics = processor.getMetrics();
            messageTypeDataset.addValue(metrics.highPriorityMessagesProcessed(), "High-Priority", processor.getMetrics().name());
            messageTypeDataset.addValue(metrics.standardMessagesProcessed(), "Standard", processor.getMetrics().name());
        }
    }
    
    private void updateMessageWaitTimeChart() {
        MessageQueue.MessageQueueMetrics metrics = messageQueue.getMetrics();
        Millisecond now = new Millisecond();
        messageWaitTimeSeries.addOrUpdate(now, metrics.averageWaitTimeMs());
    }
    
    public void shutdown() {
        if (updateTask != null) {
            updateTask.cancel(true);
        }
        scheduler.shutdown();
        frame.dispose();
    }
}
