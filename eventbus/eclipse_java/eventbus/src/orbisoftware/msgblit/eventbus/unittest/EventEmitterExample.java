package orbisoftware.msgblit.eventbus.unittest;

import orbisoftware.msgblit.eventbus.EBEvent;
import orbisoftware.msgblit.eventbus.EBRouter;

public class EventEmitterExample extends Thread {

   EventEmitterExample(EBRouter router, int emitterID) {

      this.router = router;
      this.emitterID = emitterID;
   }

   public void run() {

      while (true) {

         EBEvent event = new EBEvent();

         event.eventID = emitterID << 2;
         event.eventCatType = 20;
         event.eventDefType = 25;
         event.eventData = "java event data";

         if (emitterID == 1)
            router.publishEvent(event, EBRouter.DistType.SHARED);
         else
            router.publishEvent(event, EBRouter.DistType.LOCAL);

         try {
            Thread.sleep(1000);
         } catch (InterruptedException e) {
         }
      }
   }

   private EBRouter router;
   private int emitterID;
}
