package com.java.learn.thread.enrty;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class CompletableFutureTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
//        CompletableFuture<Void> completablefuture = CompletableFuture.runAsync(() ->{
//            try {
//                Thread.sleep(10000L);
//                System.out.println(">>run ");
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        });
//        completablefuture.get();
//        System.out.println(">>>end ");




        // 阻塞在get
//                CompletableFuture<Integer> completablefuture = CompletableFuture.supplyAsync(() ->{
//            try {
//                System.out.println(">>run ");
//                Thread.sleep(10000L);
//                return 1;
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//                    return 2;
//                });
//
//        System.out.println(">>>end " + completablefuture.get());
//        System.out.println(">>>end ");



//        CompletableFuture<Integer> completablefuture = CompletableFuture.supplyAsync(() ->{
//            try {
//                Thread.sleep(4000L);
//                System.out.println(">>run ");
//                return 1;
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            return 2;
//        });
//        // 接受结果，然后处理结果和异常，这个有返回值，但是返回值只能是之前的结果，如果要转换用 thenApplyAsync
//        CompletableFuture<Integer> completablefuture2 = completablefuture.whenCompleteAsync((v ,e) ->{
//            System.out.println("whenCompleteAsync : "+ v);
////            System.out.println(e.getMessage());
//        });
//        System.out.println(">>> end " + completablefuture2.get()); // 阻塞
//

//
//        // 相当于转换, 可以返回不同的值
//        CompletableFuture<String> completablefuture3 =  completablefuture.thenApplyAsync((v) -> ">> " + v);
//
//        // 接受返回值，然后给一个function, 这个是没有返回值的
//        completablefuture.thenAcceptAsync(System.out::println);
//
////        System.out.println(">>>end " + completablefuture.get());


//        allof();
        feizuse();
    }

    /**
     * 以上都是阻塞的方式执行，在get的地方阻塞，下面用非阻塞的方式
     */
    public static void feizuse(){
//创建线程池
        ExecutorService executorService = Executors.newCachedThreadPool();
        //调用静态方法supplyAsync（），传入自己指定的线程池
        CompletableFuture<String> completableFuture1 = CompletableFuture.supplyAsync(new Supplier<String>() {
            int result;
            //执行异步操作
            @Override
            public String get() {
                for (int i = 0; i < 5; i++) {
                    try {
                        Thread.sleep(1000);
                        result += i;
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                //返回结果
                System.out.println(">> supplyAsync return : " + result);
                return  String.valueOf(result);
            }
        },executorService).whenComplete((s, throwable) -> {
            System.out.println(">> whenComplete : " + s);
            executorService.shutdown(); // 结束线程池
        });
        System.out.println(">>>>> end 《《《《");
    }


    /**
     * 合并多个
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void allof() throws ExecutionException, InterruptedException {
        CompletableFuture<Integer> completablefuture = CompletableFuture.supplyAsync(() ->{
            try {
                Thread.sleep(1000L);
                System.out.println(">>run1 ");
                return 1;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return 2;
        });

        CompletableFuture<Integer> completablefuture2 = CompletableFuture.supplyAsync(() ->{
            try {
                Thread.sleep(3000L);
                System.out.println(">>run2 ");
                return 1;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return 2;
        });

        CompletableFuture<Integer> completablefuture3 = CompletableFuture.supplyAsync(() ->{
            try {
                Thread.sleep(6000L);
                System.out.println(">>run3 ");
                return 1;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return 2;
        });

        CompletableFuture<Void> re = CompletableFuture.allOf(completablefuture, completablefuture2, completablefuture3);

        System.out.println(">>> end " + re.get());
    }

}
