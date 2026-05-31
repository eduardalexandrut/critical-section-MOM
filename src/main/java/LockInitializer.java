import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class LockInitializer {
    public static void main(String[] args) {
        String lockName = "database-critical-section";
        String queueName = "distributed_lock_" + lockName;

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            // Remove any queue to avoid duplicate tokens on restarts
            channel.queueDeclare(queueName, true, false, false, null);
            channel.queuePurge(queueName);

            // Inject token
            channel.basicPublish("", queueName, null, "TOKEN".getBytes());
            System.out.println("Successfully seeded 1 token into: " + queueName);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
