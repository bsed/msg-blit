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

#include "EBRouter_RT.hpp"
#include <iostream>

EBRouter::EBRouter(std::string partition, int publisherID) {

   this->partition = partition;
   this->publisherID = publisherID;
}

void EBRouter::publishEvent(EBEvent &event) {

   pubEventContainer.publisherID = event.publisherID;
   pubEventContainer.eventID = event.eventID;
   pubEventContainer.eventCatType = event.eventCatType;
   pubEventContainer.eventDefType = event.eventDefType;
   pubEventContainer.eventData = event.eventData.c_str();

   eventContainerDataWriter->write(pubEventContainer, DDS::HANDLE_NIL);
}

void EBRouter::init() {

   DDS::DomainId_t myDomain = NULL;
   DDS::DomainParticipantFactory_var dpf;
   DDS::DomainParticipant_var dp;
   DDS::Subscriber_var subscriber;
   DDS::Publisher_var publisher;

   DDS::DomainParticipantQos dpQos;
   DDS::TopicQos tQos;

   DDS::SubscriberQos sQos;
   DDS::DataReaderQos drQos;
   DDS::PublisherQos pQos;
   DDS::DataWriterQos dwQos;
   DDS::Topic_var topic;

   DDSEventChannel::EventContainerTypeSupport_var ts;

   // Create Participants
   dpf = DDS::DomainParticipantFactory::get_instance();
   dpf->get_default_participant_qos(dpQos);
   dp = dpf->create_participant(myDomain, dpQos, NULL, DDS::STATUS_MASK_NONE);

   // Create Subscriber
   dp->get_default_subscriber_qos(sQos);
   sQos.partition.name.length(1);
   sQos.partition.name[0] = partition.c_str();
   subscriber = dp->create_subscriber(sQos, NULL, DDS::STATUS_MASK_NONE);

   // Create Publisher
   dp->get_default_publisher_qos(pQos);
   pQos.partition.name.length(1);
   pQos.partition.name[0] = partition.c_str();
   publisher = dp->create_publisher(pQos, NULL, DDS::STATUS_MASK_NONE);

   // Set DataReader QoS settings
   subscriber->get_default_datareader_qos(drQos);
   drQos.reliability.kind = DDS::RELIABLE_RELIABILITY_QOS;
   drQos.history.kind = DDS::KEEP_LAST_HISTORY_QOS;
   drQos.history.depth = 30;

   // Set DataWriter QoS settings
   publisher->get_default_datawriter_qos(dwQos);
   dwQos.reliability.kind = DDS::RELIABLE_RELIABILITY_QOS;
   dwQos.history.kind = DDS::KEEP_LAST_HISTORY_QOS;
   dwQos.history.depth = 30;

   // Set Topic Qos settings
   dp->get_default_topic_qos(tQos);
   tQos.reliability.kind = DDS::RELIABLE_RELIABILITY_QOS;
   tQos.history.kind = DDS::KEEP_LAST_HISTORY_QOS;
   tQos.history.depth = 30;

   // Create Topic
   ts = new DDSEventChannel::EventContainerTypeSupport();
   ts->register_type(dp, "DDSEventChannel::EventContainer");

   topic = dp->create_topic("EventContainer", "DDSEventChannel::EventContainer",
         tQos, NULL, DDS::STATUS_MASK_NONE);

   // Create Datareader
   dataReader = subscriber->create_datareader(topic, drQos, NULL,
         DDS::STATUS_MASK_NONE);
   eventContainerDataReader = DDSEventChannel::EventContainerDataReader::_narrow(
         dataReader);

   if (dataReader == NULL)
      std::cout << "ERROR: DDS Connection failed" << std::endl;

   // Create Datawriter
   eventContainerDataWriter = DDSEventChannel::EventContainerDataWriter::_narrow(
         publisher->create_datawriter(topic, dwQos, NULL,
               DDS::STATUS_MASK_NONE));
   if (eventContainerDataWriter == NULL)
      std::cout << "ERROR: DDS Connection failed" << std::endl;

   // Create Readcondition
   readCondition = dataReader->create_readcondition(DDS::ANY_SAMPLE_STATE,
         DDS::ANY_VIEW_STATE, DDS::ALIVE_INSTANCE_STATE);
}

void EBRouter::receiveEvents(std::vector<EBEvent> &eventContainer) {

   DDSEventChannel::EventContainerSeq eventContainerSeq;
   DDSEventChannel::EventContainer rcvEventContainer;
   DDS::SampleInfoSeq infoSeq;

   eventContainerDataReader->take_w_condition(eventContainerSeq, infoSeq,
         DDS::LENGTH_UNLIMITED, readCondition);

   if (infoSeq.length() > 0) {

      for (unsigned int i = 0; i < eventContainerSeq.length(); i++) {

         if (infoSeq[i].valid_data) {

            EBEvent event;
            rcvEventContainer = eventContainerSeq[i];

            event.publisherID = rcvEventContainer.publisherID;
            event.eventID = rcvEventContainer.eventID;
            event.eventCatType = rcvEventContainer.eventCatType;
            event.eventDefType = rcvEventContainer.eventDefType;
            event.eventData = rcvEventContainer.eventData;

            eventContainer.push_back(event);
         }
      }
   }

   eventContainerDataReader->return_loan(eventContainerSeq, infoSeq);
}
