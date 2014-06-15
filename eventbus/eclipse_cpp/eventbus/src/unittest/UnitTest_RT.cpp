#include "EBRouter_RT.hpp"
#include <vector>

int main(int argc, char *argv[]) {

   EBRouter *router = new EBRouter("", 5);

   router->init();

   while (true) {

      std::vector<EBEvent> eventContainer;
      EBEvent event;
      int size;

      eventContainer.clear();
      router->receiveEvents(eventContainer);
      size = eventContainer.size();

      for (int i = 0; i < size; i++) {

         event = eventContainer[i];
         std::cout << "Event info:" << std::endl;
         std::cout << "publisherID = " << event.eventID << std::endl;
         std::cout << "eventID = " << event.eventCatType << std::endl;
         std::cout << "eventCatType = " << event.eventCatType << std::endl;
         std::cout << "eventDefType = " << event.eventDefType << std::endl;
         std::cout << "eventData = " << event.eventData << std::endl << std::endl;
      }

      event.eventID = 4;
      event.eventCatType = 11;
      event.eventDefType = 12;
      event.eventData = "C++ event data";

      router->publishEvent(event);

      usleep(1000000);
   }

   return 0;
}
