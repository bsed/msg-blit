/*
 *  msg-blit - Implementation of various messaging patterns.
 *  Event Bus - Simple implementation of an event bus utilizing
 *  observer pattern.
 *
 *  Copyright (C) 2014 Harlan Murphy
 *  orbisoftware@gmail.com
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License Version 3 dated 29 June 2007, as published by the
 *  Free Software Foundation.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with restful-dds; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */

#ifndef EB_ROUTER_HPP_
#define EB_ROUTER_HPP_

#include <ccpp_dds_dcps.h>
#include <DDSEventChannel/DDSEventChannel.h>
#include <DDSEventChannel/ccpp_DDSEventChannel.h>
#include <boost/thread.hpp>
#include <EBEvent.hpp>
#include <SharedQueue.hpp>
#include <EBSubscriber.hpp>

class EBRouter {

public:

   // Used to specify if a variable should be published over network
   // or if it should only be stored locally
   enum DistType {

      SHARED, LOCAL
   };

   // Create object using specified partition and publisherID
   EBRouter(std::string partition, int publisherID);

   // Add event subscriber
   void addSubscriber(std::string subscriberName,
         EBSubscriber *eventSubscriber);

   // Remove event subscriber
   void removeSubscriber(std::string subscriberName,
         EBSubscriber *eventSubscriber);

   // Publish event
   void publishEvent(EBEvent event, DistType distType);

   // Start request that must be called prior to any other methods
   void startReq();

   // Shutdown request
   void shutdownReq();

   // Returns true when thread is complete
   bool threadIsComplete();

private:

   void init();
   void run();
   void notifySubscribers(EBEvent event);
   void publishPending();

   boost::thread ebRouterThread;

   DDS::DataReader_ptr dataReader;
   DDS::ReadCondition_ptr readCondition;
   DDS::WaitSet_ptr waitSet;

   DDSEventChannel::EventContainerDataReader_ptr transactionDataReader;
   DDSEventChannel::EventContainerDataWriter_ptr transactionDataWriter;
   DDSEventChannel::EventContainer pubEventContainer;

   bool shutdownRequested;
   bool threadHasComplete;
   std::string partition;
   int publisherID;

   SharedQueue<EBEvent> pubQueue;
   std::map<std::string, EBSubscriber *> subscribers;
   std::map<std::string, EBSubscriber *>::iterator subIterator;
};

#endif /* EB_ROUTER_HPP_ */
