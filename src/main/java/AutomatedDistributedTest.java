import java.io.IOException;

public class AutomatedDistributedTest {
    private static final int NUM_PROCESSES = 5;

    public static void main(String[] args) throws IOException, InterruptedException {
        LockInitializer.main(args);
        System.out.println("==== Initialization complete. Spawning multiple processes. ====");

        // Create command
        String javaHome = System.getProperty("java.home");
        String javaCmd = javaHome + "/bin/java";
        String classpath = System.getProperty("java.class.path");

        String className = "TestProcess";

        // Spawn multiple Processes
        Process[] processes = new Process[NUM_PROCESSES];
        for (int i = 0; i < NUM_PROCESSES; i++) {
            ProcessBuilder pb = new ProcessBuilder(javaCmd, "-cp", classpath, className);
            pb.inheritIO();
            processes[i] = pb.start();
            Thread.sleep(500); // For readable logs
        }

        // Wait for processes to complete
        for (Process p : processes) {
            p.waitFor();
        }

        System.out.println("==== All distributed processes completed successfully. ====");
    }
}
