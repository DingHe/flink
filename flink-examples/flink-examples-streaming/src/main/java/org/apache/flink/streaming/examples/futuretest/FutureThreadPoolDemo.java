package org.apache.flink.streaming.examples.futuretest;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

public class FutureThreadPoolDemo {
    public static void main(String[] args) throws InterruptedException, ExecutionException {

        long startTime = System.currentTimeMillis();
        FutureTask<String> futureTask1 = new FutureTask<>(() -> {
            TimeUnit.MILLISECONDS.sleep(500);
            return "task1 over";
        });

        FutureTask<String> futureTask2 = new FutureTask<>(() -> {
            TimeUnit.MILLISECONDS.sleep(300);
            return "task2 over";
        });

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.submit(futureTask1);
        executorService.submit(futureTask2);
        //第三个线程
        TimeUnit.MILLISECONDS.sleep(500);

        System.out.println(futureTask1.get());
        System.out.println(futureTask2.get());

        long endTime = System.currentTimeMillis();
        System.out.println("耗时："+(endTime - startTime));
        executorService.shutdown();

    }
}
