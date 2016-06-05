package com.example.aggregatorapp;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

public class Aggregator {
    private ConcurrentLinkedQueue<AggregatorTask> tasks = new ConcurrentLinkedQueue<>();

    private HashMap<String, Integer> map = new HashMap<>();
    private final Object mapLock = new Object();

    private final int threadCount = 5;

    public void setTasks(List<AggregatorTask> tasks) {
        this.tasks.addAll(tasks);
    }

    private void performTask(AggregatorTask task) {
        synchronized (mapLock) {
            // increment or initialize with 1
            map.put(task.Key, map.getOrDefault(task.Key, 0) + 1);
        }
    }

    private Thread startThread(CountDownLatch latch) {
        Thread thread = new Thread() {
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
        };

        thread.start();

        return thread;
    }

    public void aggregate() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            startThread(latch);
        }

        latch.await();

        for (String key : map.keySet()) {
            System.out.printf("%s => %d\n", key, map.get(key));
        }
    }
}
