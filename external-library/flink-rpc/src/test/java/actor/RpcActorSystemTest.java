package actor;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

public class RpcActorSystemTest {
    // 通信系统已启动就会阻塞住，所以在TM上main函数执行完也不会退出
    public static void main(String[] args) {
         ActorSystem actorSystem = ActorSystem.create();
        ActorRef angryFoal = actorSystem.actorOf(Props.create(ReciveActor.class));
        ActorRef lazyFoal = actorSystem.actorOf(Props.create(SendActor.class));
        angryFoal.tell("hello! I am  LazyFoalActor!", lazyFoal);
    }
}
