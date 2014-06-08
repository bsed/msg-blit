package orbisoftware.msgblit.eventbus.unittest;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Queue;

import orbisoftware.msgblit.eventbus.EBEvent;
import orbisoftware.msgblit.eventbus.EBSubscriber;

public class LoggerExample implements EBSubscriber {

   LoggerExample() {

      rcvQueue = new ConcurrentLinkedQueue<EBEvent>();
   }

   @Override
   public void sinkEvent(EBEvent event) {

      if (rcvQueue.size() < maxQueueSize)
         rcvQueue.add(event);
   }

   public boolean hasEvents() {

      return rcvQueue.size() > 0;
   }

   public void processEvents() {

      int size = rcvQueue.size();

      for (int i = 0; i < size; i++) {

         EBEvent event = rcvQueue.poll();

         if (event != null) {

            System.out.println("\nEvent info:");
            System.out.println("publisherID = " + event.publisherID);
            System.out.println("eventID = " + event.eventID);
            System.out.println("eventCatType = " + event.eventCatType);
            System.out.println("eventDefType = " + event.eventDefType);
            System.out.println("eventData = " + event.eventData);
         }
      }
   }

   private Queue<EBEvent> rcvQueue;
   private int maxQueueSize = 100;
}
