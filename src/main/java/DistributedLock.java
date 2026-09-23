import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class DistributedLock implements AutoCloseable {
    private String queName;
    private Channel channel;
    private Connection connection;
    private long lastDelivery = 0;

    public DistributedLock(String lockName, String rabbitMqHost) {
        this.queName = "distributed_lock_" + lockName;

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(rabbitMqHost);

        try {
            this.connection = factory.newConnection();
            this.channel = this.connection.createChannel();

            this.channel.basicQos(1);
            this.channel.queueDeclare(queName, true, false, false, null);
        } catch (IOException | TimeoutException e) {
            throw new RuntimeException("Failed to initialize RabbitMQ connection", e);
        }

    }

    public void lock() throws IOException, InterruptedException {
        CompletableFuture<Long> tokenFuture = new CompletableFuture<>();

        String consumerTag = channel.basicConsume(queName, false,
                (consumerTagStr, delivery) -> {
                    // Complete the future with the delivery tag of the token
                    if (!tokenFuture.isDone()) {
                        tokenFuture.complete(delivery.getEnvelope().getDeliveryTag());
                    }
                },
                (consumerTagStr) -> { }
        );

        try {
            this.lastDelivery = tokenFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Failed while waiting to acquire lock", e);
        } finally {
            // Unregister the consumer immediately so we don't accidentally grab it
            // again the moment we release it.
            channel.basicCancel(consumerTag);
        }
    }

    public void unlock() throws IOException, InterruptedException {
        if (lastDelivery == 0) {
            throw new IllegalStateException("Cannot unlock: Lock was not acquired by this instance.");
        }

        // Atomically return the token back to the queue for the next process.
        channel.basicReject(lastDelivery, true);

        this.lastDelivery = 0;
    }

    @Override
    public void close() throws Exception {

        if (channel != null && channel.isOpen()) {
            channel.close();
        }

        if (connection != null && connection.isOpen()) {
            connection.close();
        }
    }
}
