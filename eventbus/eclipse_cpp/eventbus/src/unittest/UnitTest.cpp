#include <boost/thread.hpp>
#include <EventEmitterExample.hpp>
#include <LoggerExample.hpp>

static bool shutdown = false;

void sigint_handler(int sig) {

   shutdown = true;
}

int main(int argc, char *argv[]) {

   signal(SIGINT, sigint_handler);

   EBRouter *router = new EBRouter("", 5);
   EventEmitterExample* eventEmitter1 = new EventEmitterExample(router, 1);
   EventEmitterExample* eventEmitter2 = new EventEmitterExample(router, 2);
   LoggerExample* logger = new LoggerExample();

   router->addSubscriber("logger", logger);
   router->startReq();

   eventEmitter1->start();
   eventEmitter2->start();

   while (true) {

      if (logger->hasEvents())
         logger->processEvents();

      if (shutdown) {

         router->shutdownReq();

         while (!router->threadIsComplete())
            boost::this_thread::sleep(boost::posix_time::milliseconds(200));

         abort();
      }

      boost::this_thread::sleep(boost::posix_time::milliseconds(1000));
   }

   return 0;
}
