package com.tesco.price.kafkaconsumer;

import java.util.Scanner;

public class application {
    public static void main(String[] args){
        Scanner scanner = new Scanner(System.in);
        boolean done = false;

        Consumer consumer = new Consumer();
        Thread thread = new Thread(consumer);

        thread.start();

        while(!done){
            done = "q".equals(scanner.next());
        }

        consumer.done();

        try {
            thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("Done!");
    }
}
