
#include <LoggerExample.hpp>

   LoggerExample::LoggerExample() {

   }

   void LoggerExample::sinkEvent(EBEvent event) {

      rcvQueue.push(event);
   }

   bool LoggerExample::hasEvents() {

      return rcvQueue.size() > 0;
   }

   void LoggerExample::processEvents() {

      int size = rcvQueue.size();

      for (int i = 0; i < size; i++) {

         EBEvent event;

         if (rcvQueue.front(event)) {

            std::cout << "\nEvent info:" << std::endl;
            std::cout << "publisherID = " << event.publisherID << std::endl;
            std::cout << "eventID = " << event.eventID << std::endl;
            std::cout << "eventCatType = "<< event.eventCatType << std::endl;
            std::cout << "eventDefType = " << event.eventDefType << std::endl;
            std::cout << "eventData = " << event.eventData << std::endl;
         }
      }
   }
