package com.example.aggregatorapp;

import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;

public class AggregatorRunnable implements Runnable {
    private final Queue<AggregatorTask> tasks;
    private final HashMap<String, Integer> map;
    private final CountDownLatch latch;
    private final Object mapAccessLock;

    public AggregatorRunnable(Queue<AggregatorTask> tasks, HashMap<String, Integer> map, Object mapAccessLock, CountDownLatch latch) {
        this.tasks = tasks;
        this.map = map;
        this.mapAccessLock = mapAccessLock;
        this.latch = latch;
    }

    @Override
    public void run() {
        while (true) {
            AggregatorTask task = tasks.poll();

            if (task == null) {
                break;
            }

            performTask(task);
        }

        latch.countDown();
    }

    private void performTask(AggregatorTask task) {
        synchronized (mapAccessLock) {
            // increment or initialize with 1
            map.put(task.Key, map.getOrDefault(task.Key, 0) + 1);
        }
    }
}
