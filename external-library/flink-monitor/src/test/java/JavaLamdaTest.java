import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class JavaLamdaTest {

    /**
     * 将func作为参数
     */
    @Test
    public void testFunc(){
        Consumer<Object> print = (x) -> System.out.println(x);
        // 方法
        Function<Integer, String> f = (id) -> id.toString();
        // 写法2 :: 将toString
        // Function<Integer, String> f2 = Objects::toString;

        // 消费
        print.accept(IntToString(1, f));


        // 如果不想写接口就用 Function<输入，输出>
        MathOperation func1 = (x, y) -> x+ y ;
        print.accept(this.operate(1, 2, func1));


    }
    /**
     * 类似scala的 .map.filter
     */
    @Test
    public void testStream(){
        List<String> l = new ArrayList<>();
        l.add("1"); l.add("2");
        List<String> a = l.stream()
                .filter(x ->  x.equals("1"))
                .map(x -> x+": hello")
                .collect(Collectors.toList());
        a.forEach(System.out::println);
    }


    public String IntToString(Integer i, Function<Integer, String>  f ) {
        return f.apply(i);
    }

    public int operate(int a, int b, MathOperation mathOperation){
        return mathOperation.operation(a, b);
    }

    interface MathOperation {
        int operation(int a, int b);
    }

}
