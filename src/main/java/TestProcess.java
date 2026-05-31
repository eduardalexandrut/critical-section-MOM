import java.util.Random;

public class TestProcess {
    public static void main(String[] args) {
        String processId = "Process-" + new Random().nextInt(1000);

        Thread.ofVirtual().start(() -> {
            try (DistributedLock lock = new DistributedLock("database-critical-section", "localhost")) {

                for (int i = 0; i < 3; i++) {
                    System.out.println("[" + processId + "] Requesting critical section entry...");

                    lock.lock(); // Blocks if another terminal window has it
                    System.out.println(">>> [" + processId + "] ENTERED critical section!");
                    // Simulate doing critical database/file I/O work
                    Thread.sleep(3000);
                    System.out.println("<<< [" + processId + "] LEAVING critical section.");
                    lock.unlock();

                    Thread.sleep(2000);
                }
                System.out.println("[" + processId + "] Execution finished successfully!");

            } catch (Exception e) {
                System.err.println("Error in distributed execution: " + e.getMessage());
            }
        });

        try { Thread.sleep(20000); } catch (InterruptedException ignored) {}
    }
}