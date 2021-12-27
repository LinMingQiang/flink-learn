package actor;

import akka.actor.UntypedActor;

public class SendActor extends UntypedActor {
    @Override
    public void onReceive(Object message) throws Exception {
        System.out.println("LazyFoalActor receive message : " + message);
    }
}
