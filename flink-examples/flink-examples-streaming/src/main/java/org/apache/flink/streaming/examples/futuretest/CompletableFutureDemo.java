package org.apache.flink.streaming.examples.futuretest;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

public class CompletableFutureDemo {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        FutureTask<String> futureTask = new FutureTask<>(new MyTrhead());
        Thread t1 = new Thread(futureTask,"t1");
        t1.start();

        System.out.println(futureTask.get());
    }
}

class MyTrhead implements Callable<String>{
    @Override
    public String call() throws Exception {
        System.out.println("----------come in call()");
        return "hello callable";
    }
}
