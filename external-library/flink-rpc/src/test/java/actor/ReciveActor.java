package actor;

import akka.actor.AbstractActor;
import akka.actor.Actor;
import akka.actor.UntypedActor;

public class ReciveActor extends UntypedActor {

    @Override
    public void onReceive(Object message) throws Throwable, Throwable {
        System.out.println("AngryFoalActor receive message : " + message);
        getSender().tell("hello! I am  AngryFoalActor!", getSelf());
    }
}
