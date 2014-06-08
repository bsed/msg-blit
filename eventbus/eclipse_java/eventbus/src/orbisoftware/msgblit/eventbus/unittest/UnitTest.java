package orbisoftware.msgblit.eventbus.unittest;

import orbisoftware.msgblit.eventbus.EBRouter;

public class UnitTest {

   public static void main(String args[]) {

      EBRouter router = new EBRouter("", 30);
      EventEmitterExample eventEmitter1 = new EventEmitterExample(router, 1);
      EventEmitterExample eventEmitter2 = new EventEmitterExample(router, 2);
      LoggerExample logger = new LoggerExample();
      
      router.addSubscriber("logger", logger);
      router.start();

      eventEmitter1.start();
      eventEmitter2.start();

      while (true) {

         if (logger.hasEvents())
            logger.processEvents();

         try {
            Thread.sleep(1000);
         } catch (InterruptedException e) {
         }
      }
   }
}
