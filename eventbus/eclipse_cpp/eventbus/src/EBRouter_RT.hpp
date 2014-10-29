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

#ifndef EB_ROUTER_RT_HPP_
#define EB_ROUTER_RT_HPP_

#include <ccpp_dds_dcps.h>
#include <DDSEventChannel/DDSEventChannel.h>
#include <DDSEventChannel/ccpp_DDSEventChannel.h>
#include <EBEvent.hpp>
#include <SharedQueue.hpp>
#include <vector>

class EBRouter {

public:

   // Create object using specified partition and publisherID
   EBRouter(std::string partition, int publisherID);

   // Initialize Event Bus Router
   void init();

   // Publish event
   void publishEvent(EBEvent &event);

   // Receive available events published on event bus
   void receiveEvents(std::vector<EBEvent> &events);

private:

   DDS::DataReader_ptr dataReader;
   DDS::ReadCondition_ptr readCondition;

   DDSEventChannel::EventContainerDataReader_ptr eventContainerDataReader;
   DDSEventChannel::EventContainerDataWriter_ptr eventContainerDataWriter;
   DDSEventChannel::EventContainer pubEventContainer;

   std::string partition;
   int publisherID;
};

#endif /* EB_ROUTER_RT_HPP_ */
