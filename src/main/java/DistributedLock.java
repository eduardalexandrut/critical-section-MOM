import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class DistributedLock implements AutoCloseable {
    private String queName;
    private Channel channel;
    private Connection connection;
    private long lastDelivery;

    public DistributedLock(String lockName, String rabbitMqHost) {
        this.queName = "distributed_lock_" + lockName;

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(rabbitMqHost);

        try {
            this.connection = factory.newConnection();
            this.channel = this.connection.createChannel();

            this.channel.queueDeclare(queName, true, false, false, null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }

    }

    public void lock() throws IOException, InterruptedException {
        while (true) {
            GetResponse getResponse = channel.basicGet(queName, false);

            if (getResponse != null) {
                this.lastDelivery = getResponse.getEnvelope().getDeliveryTag();
                return;
            }
            Thread.sleep(1000);
        }
    }

    public void unlock() throws IOException, InterruptedException {
        if (lastDelivery == 0) {
            throw new IllegalStateException("Cannot unlock: Lock was not acquired by this instance.");
        }

        channel.basicPublish("", queName, null, "TOKEN".getBytes());
        channel.basicAck(lastDelivery, false);

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
