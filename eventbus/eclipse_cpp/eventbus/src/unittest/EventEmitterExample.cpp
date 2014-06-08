#include <EventEmitterExample.hpp>
#include <boost/thread.hpp>
#include <string>

EventEmitterExample::EventEmitterExample(EBRouter* router, int emitterID) {

   this->router = router;
   this->emitterID = emitterID;
}

void EventEmitterExample::start() {
   boost::thread(&EventEmitterExample::run, this);
}

void EventEmitterExample::run() {

   while (true) {

      EBEvent event;

      event.eventID = emitterID << 4;
      event.eventCatType = 11;
      event.eventDefType = 12;
      event.eventData = "C++ event data";

      if (emitterID == 1)
         router->publishEvent(event, EBRouter::SHARED);
      else
         router->publishEvent(event, EBRouter::LOCAL);

      boost::this_thread::sleep(boost::posix_time::milliseconds(1000));
   }
}

