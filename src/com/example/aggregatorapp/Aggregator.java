package com.example.aggregatorapp;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

public class Aggregator {
    private final ConcurrentLinkedQueue<AggregatorTask> tasks = new ConcurrentLinkedQueue<>();
    private int threadCount = 1;

    public void setThreadCount(int count) throws Exception {
        if (count <= 0) {
            throw new Exception("Parameter 'count' must be greater than zero");
        }

        threadCount = count;
    }

    public void addTasks(List<AggregatorTask> tasks) {
        this.tasks.addAll(tasks);
    }

    public void aggregate() throws InterruptedException {
        final HashMap<String, Integer> map = new HashMap<>();
        final Object mapAccessLock = new Object();
        final CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            new Thread(new AggregatorRunnable(tasks, map, mapAccessLock, latch)).start();
        }

        latch.await();

        for (String key : map.keySet()) {
            System.out.printf("%s => %d\n", key, map.get(key));
        }
    }
}
