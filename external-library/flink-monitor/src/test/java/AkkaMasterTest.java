//import akka.actor.*;
//import com.typesafe.config.Config;
//import com.typesafe.config.ConfigFactory;
//import org.apache.flink.runtime.rpc.RpcEndpoint;
//import org.apache.flink.runtime.rpc.RpcGateway;
//import org.apache.flink.runtime.rpc.RpcService;
//import scala.Option;
//import scala.PartialFunction;
//import scala.runtime.BoxedUnit;
//import scala.sys.Prop;
//
//import java.util.HashMap;
//
//public class AkkaMasterTest implements Actor {
//    public AkkaMasterTest() {
//        super();
//    }
//
//    public static void main(String[] args) {
////        String configStr ="akka.actor.provider = akka.remote.RemoteActorRefProvider"+"\n"+
////       "akka.remote.netty.tcp.hostname = localhost"+"\n"+
////       "akka.remote.netty.tcp.port = 21231";
//        HashMap configStr = new HashMap<String, String>();
//        configStr.put("akka.actor.provider", "akka.remote.RemoteActorRefProvider");
//        configStr.put("akka.remote.netty.tcp.hostname", "localhost");
//        configStr.put("akka.remote.netty.tcp.port", "21231");
//        Config config = ConfigFactory.parseMap(configStr);
//        ActorSystem system = ActorSystem.create("testSystem", config);
//        ActorRef master = system.actorOf(Props.create(AkkaMasterTest.class), "xx");
//        master.tell("hello", ActorRef.noSender());
//        System.out.println(system);
//    }
//
//    @Override
//    public ActorContext context() {
//        return null;
//    }
//
//    @Override
//    public ActorRef self() {
//        return null;
//    }
//
//    @Override
//    public ActorRef sender() {
//        return Actor.super.sender();
//    }
//
//    @Override
//    public PartialFunction<Object, BoxedUnit> receive() {
//        System.out.println(">>>");
//        return null;
//    }
//
//    @Override
//    public void aroundReceive(PartialFunction<Object, BoxedUnit> receive, Object msg) {
//        Actor.super.aroundReceive(receive, msg);
//    }
//
//    @Override
//    public void aroundPreStart() {
//        Actor.super.aroundPreStart();
//    }
//
//    @Override
//    public void aroundPostStop() {
//        Actor.super.aroundPostStop();
//    }
//
//    @Override
//    public void aroundPreRestart(Throwable reason, Option<Object> message) {
//        Actor.super.aroundPreRestart(reason, message);
//    }
//
//    @Override
//    public void aroundPostRestart(Throwable reason) {
//        Actor.super.aroundPostRestart(reason);
//    }
//
//    @Override
//    public SupervisorStrategy supervisorStrategy() {
//        return Actor.super.supervisorStrategy();
//    }
//
//    @Override
//    public void preStart() throws Exception, Exception {
//        Actor.super.preStart();
//    }
//
//    @Override
//    public void postStop() throws Exception, Exception {
//        Actor.super.postStop();
//    }
//
//    @Override
//    public void preRestart(Throwable reason, Option<Object> message) throws Exception, Exception {
//        Actor.super.preRestart(reason, message);
//    }
//
//    @Override
//    public void postRestart(Throwable reason) throws Exception, Exception {
//        Actor.super.postRestart(reason);
//    }
//
//    @Override
//    public void unhandled(Object message) {
//        Actor.super.unhandled(message);
//    }
//}
