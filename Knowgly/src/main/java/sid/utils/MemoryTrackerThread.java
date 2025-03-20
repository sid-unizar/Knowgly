package sid.utils;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;

/**
 * Simple memory tracker which logs the biggest heap usage during execution. Runs a check every minute until interrupted
 * or stopTracking() is run
 */
public class MemoryTrackerThread extends Thread {
    private volatile boolean stop;
    public long maxMemoryUsage;

    public MemoryTrackerThread() {
        this.stop = false;
        this.maxMemoryUsage = 0;
    }

    public void stopTracking() {
        this.stop = true;
        interrupt();
    }

    @Override
    public void run() {
        while (!stop) {
            System.gc();

            MemoryUsage memoryUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
            long usedMemory = memoryUsage.getUsed();

            if (usedMemory > maxMemoryUsage) {
                maxMemoryUsage = usedMemory;
            }

            try {
                Thread.sleep(60 * 1000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }
}