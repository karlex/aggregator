package com.example.aggregatorapp;

import java.util.ArrayList;
import java.util.List;

public class AggregatorApp {
    public static void main(String[] args) throws Exception {
        List<AggregatorTask> tasks = prepareTasks();

        Aggregator aggregator = new Aggregator();
        aggregator.addTasks(tasks);
        aggregator.setThreadCount(5);
        aggregator.aggregate();
    }

    private static List<AggregatorTask> prepareTasks() {
        ArrayList<AggregatorTask> tasks = new ArrayList<>();

        ArrayList<String> keys = new ArrayList<>();
        keys.add("aaa");
        keys.add("bbb");
        keys.add("ccc");

        for (int i = 0; i < 1000; i++) {
            AggregatorTask task = new AggregatorTask();
            task.Id = i + 1;
            task.Key = keys.get(Math.round((float)Math.random() * 2));
            task.Timestamp = Math.round((float)Math.random() * 100500);

            tasks.add(task);
        }

        return tasks;
    }
}
