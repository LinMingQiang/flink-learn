import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

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




//                CompletableFuture<Integer> completablefuture = CompletableFuture.supplyAsync(() ->{
//            try {
//                Thread.sleep(10000L);
//                System.out.println(">>run ");
//                return 1;
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//                    return 2;
//                });
//
//        System.out.println(">>>end " + completablefuture.get());



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
//
//        // 接受结果，然后处理结果和异常，这个有返回值，但是返回值只能是之前的结果，如果要转换用 thenApplyAsync
//        CompletableFuture<Integer> completablefuture2 = completablefuture.whenCompleteAsync((v ,e) ->{
//            System.out.println(v);
//            System.out.println(e.getMessage());
//        });
//
//        // 相当于转换, 可以返回不同的值
//        CompletableFuture<String> completablefuture3 =  completablefuture.thenApplyAsync((v) -> ">> " + v);
//
//        // 接受返回值，然后给一个function, 这个是没有返回值的
//        completablefuture.thenAcceptAsync(System.out::println);
//
////        System.out.println(">>>end " + completablefuture.get());


        allof();
    }



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
