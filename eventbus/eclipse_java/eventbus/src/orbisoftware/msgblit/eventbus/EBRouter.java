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

package orbisoftware.msgblit.eventbus;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class EBRouter extends Thread {

   // Used to specify if an event should be published over network
   // or if it should only be processed locally
   public enum DistType {

      SHARED, LOCAL
   }

   // Create object using specified partition and publisherID
   public EBRouter(String partition, int publisherID) {

      subscribers = new HashMap<String, EBSubscriber>();
      pubEventContainer = new DDSEventChannel.EventContainer();
      pubQueue = new ConcurrentLinkedQueue<EBEvent>();
      this.partition = new String();

      shutdownRequested = false;
      threadHasComplete = false;

      this.partition = partition;
      this.publisherID = publisherID;
   }

   // Add event subscriber
   public void addSubscriber(String subscriberName, EBSubscriber eventSubscriber) {

      subscribers.put(subscriberName, eventSubscriber);
   }

   // Remove event subscriber
   public void removeSubscriber(String subscriberName,
         EBSubscriber eventSubscriber) {

      subscribers.remove(subscriberName);
   }

   // Publish event
   public void publishEvent(EBEvent event, DistType distType) {

      event.publisherID = publisherID;

      if (distType == DistType.SHARED) {
         pubQueue.add(event);
      } else {
         notifySubscribers(event);
      }
   }

   // Shutdown request
   public void shutdownReq() {

      shutdownRequested = true;
   }

   // Returns true when thread is complete
   public boolean threadIsComplete() {

      return threadHasComplete;
   }

   private void init() {

      String myDomain = null;
      DDS.DomainParticipantFactory dpf;
      DDS.DomainParticipant dp;
      DDS.Subscriber subscriber;
      DDS.Publisher publisher;

      DDS.DomainParticipantQosHolder dpQos;
      DDS.TopicQosHolder tQos;

      DDS.SubscriberQosHolder sQos;
      DDS.DataReaderQosHolder drQos;
      DDS.PublisherQosHolder pQos;
      DDS.DataWriterQosHolder dwQos;
      DDS.Topic topic;

      DDSEventChannel.EventContainerTypeSupport ts;

      // Create Qos holders
      dpQos = new DDS.DomainParticipantQosHolder();
      tQos = new DDS.TopicQosHolder();
      sQos = new DDS.SubscriberQosHolder();
      pQos = new DDS.PublisherQosHolder();
      drQos = new DDS.DataReaderQosHolder();
      dwQos = new DDS.DataWriterQosHolder();

      // Create Participants
      dpf = DDS.DomainParticipantFactory.get_instance();
      dpf.get_default_participant_qos(dpQos);
      dp = dpf.create_participant(myDomain, dpQos.value, null,
            DDS.STATUS_MASK_NONE.value);

      // Create Subscriber
      dp.get_default_subscriber_qos(sQos);
      sQos.value.partition.name = new String[1];
      sQos.value.partition.name[0] = partition;
      subscriber = dp.create_subscriber(sQos.value, null,
            DDS.STATUS_MASK_NONE.value);

      // Create Publisher
      dp.get_default_publisher_qos(pQos);
      pQos.value.partition.name = new String[1];
      pQos.value.partition.name[0] = partition;
      publisher = dp.create_publisher(pQos.value, null,
            DDS.STATUS_MASK_NONE.value);

      // Set DataReader QoS settings
      subscriber.get_default_datareader_qos(drQos);
      drQos.value.reliability.kind = DDS.ReliabilityQosPolicyKind.RELIABLE_RELIABILITY_QOS;
      drQos.value.history.kind = DDS.HistoryQosPolicyKind.KEEP_LAST_HISTORY_QOS;
      drQos.value.history.depth = 30;

      // Set DataWriter QoS settings
      publisher.get_default_datawriter_qos(dwQos);
      dwQos.value.reliability.kind = DDS.ReliabilityQosPolicyKind.RELIABLE_RELIABILITY_QOS;
      dwQos.value.history.kind = DDS.HistoryQosPolicyKind.KEEP_LAST_HISTORY_QOS;
      dwQos.value.history.depth = 30;

      // Set Topic Qos settings
      dp.get_default_topic_qos(tQos);
      tQos.value.reliability.kind = DDS.ReliabilityQosPolicyKind.RELIABLE_RELIABILITY_QOS;
      tQos.value.history.kind = DDS.HistoryQosPolicyKind.KEEP_LAST_HISTORY_QOS;
      tQos.value.history.depth = 30;

      // Create Topic
      ts = new DDSEventChannel.EventContainerTypeSupport();
      ts.register_type(dp, "DDSEventChannel::EventContainer");

      topic = dp.create_topic("EventContainer",
            "DDSEventChannel::EventContainer", tQos.value, null,
            DDS.STATUS_MASK_NONE.value);

      // Create Datareader
      dataReader = subscriber.create_datareader(topic, drQos.value, null,
            DDS.STATUS_MASK_NONE.value);
      eventContainerDataReader = DDSEventChannel.EventContainerDataReaderHelper
            .narrow(dataReader);

      if (dataReader == null)
         System.err.println("ERROR: DDS Connection failed");

      // Create Datawriter
      eventContainerDataWriter = DDSEventChannel.EventContainerDataWriterHelper
            .narrow(publisher.create_datawriter(topic, dwQos.value, null,
                  DDS.STATUS_MASK_NONE.value));

      if (eventContainerDataWriter == null)
         System.err.println("ERROR: DDS Connection failed");

      // Create Readcondition
      readCondition = dataReader.create_readcondition(
            DDS.ANY_SAMPLE_STATE.value, DDS.ANY_VIEW_STATE.value,
            DDS.ALIVE_INSTANCE_STATE.value);
   }

   public void run() {

      DDSEventChannel.EventContainerSeqHolder eventContainerSeq = new DDSEventChannel.EventContainerSeqHolder();
      DDSEventChannel.EventContainer rcvEventContainer;
      DDS.SampleInfoSeqHolder infoSeq = new DDS.SampleInfoSeqHolder();

      init();

      while (!shutdownRequested) {

         eventContainerDataReader.take_w_condition(eventContainerSeq, infoSeq,
               DDS.LENGTH_UNLIMITED.value, readCondition);

         if (infoSeq.value != null && infoSeq.value.length > 0) {

            for (int i = 0; i < eventContainerSeq.value.length; i++) {

               if (infoSeq.value[i].valid_data) {

                  EBEvent event = new EBEvent();
                  rcvEventContainer = eventContainerSeq.value[i];

                  event.publisherID = rcvEventContainer.publisherID;
                  event.eventID = rcvEventContainer.eventID;
                  event.eventCatType = rcvEventContainer.eventCatType;
                  event.eventDefType = rcvEventContainer.eventDefType;
                  event.eventData = rcvEventContainer.eventData;

                  notifySubscribers(event);
               }
            }
         }

         eventContainerDataReader.return_loan(eventContainerSeq, infoSeq);

         publishPending();
         
         try {
            Thread.sleep(30);
         } catch (InterruptedException ex) { }
      }

      threadHasComplete = true;
   }

   private void notifySubscribers(EBEvent event) {

      for (EBSubscriber subscriber : subscribers.values()) {

         subscriber.sinkEvent(event);
      }
   }

   private void publishPending() {

      int size = pubQueue.size();

      if (size > 0) {

         for (int i = 0; i < size; i++) {

            EBEvent event = pubQueue.poll();

            if (event != null) {

               pubEventContainer.publisherID = event.publisherID;
               pubEventContainer.eventID = event.eventID;
               pubEventContainer.eventCatType = event.eventCatType;
               pubEventContainer.eventDefType = event.eventDefType;
               pubEventContainer.eventData = event.eventData;

               eventContainerDataWriter.write(pubEventContainer,
                     DDS.HANDLE_NIL.value);
            }
         }
      }
   }

   private DDS.DataReader dataReader;
   private DDS.ReadCondition readCondition;

   private DDSEventChannel.EventContainerDataReader eventContainerDataReader;
   private DDSEventChannel.EventContainerDataWriter eventContainerDataWriter;
   private DDSEventChannel.EventContainer pubEventContainer;

   private boolean shutdownRequested;
   private boolean threadHasComplete;
   private String partition;
   private int publisherID;

   private Queue<EBEvent> pubQueue;
   private Map<String, EBSubscriber> subscribers;
}
