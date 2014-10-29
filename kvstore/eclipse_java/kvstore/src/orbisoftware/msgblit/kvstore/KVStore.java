/*
 *  msg-blit - Implementation of various messaging patterns.
 *  KVStore - Simple Key Value Store for a common set of
 *  types and arrays.
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

package orbisoftware.msgblit.kvstore;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import orbisoftware.msgblit.kvstore.util.ByteSwapper;

import DDSKVStore.*;

public class KVStore extends Thread {

   // Used to specify if a variable should be published over network
   // or if it should only be stored locally
   public enum DistType {

      SHARED, LOCAL
   }

   // Create object using specified partition and publisherID
   public KVStore(String partition, int publisherID) {

      dataStore = new HashMap<String, KVSObject>();
      pubTransaction = new DDSKVStore.Transaction();
      pubQueue = new ConcurrentLinkedQueue<KVSObject>();
      this.partition = new String();

      shutdownRequested = false;
      threadHasComplete = false;

      systemByteOrder = ARCH_Info.ARCH_BIG_ENDIAN;
      this.partition = partition;
      this.publisherID = publisherID;
   }

   // Shutdown request
   public void shutdownReq() {

      shutdownRequested = true;
   }

   // Returns true when thread is complete
   public boolean threadIsComplete() {

      return threadHasComplete;
   }

   public synchronized void setInt8Value(String key, byte value, DistType varType) {

      KVSObject kvsObject = new KVSObject();

      kvsObject.key = key;
      kvsObject.typeInfo = TYPE_Info.TYPE_INT8;
      kvsObject.numberElements = 1;
      kvsObject.byteBuffer = ByteBuffer.allocate(1).put(value).array();

      dataStore.put(key, kvsObject);

      if (varType == DistType.SHARED)
         pubQueue.add(kvsObject);
   }

   public synchronized byte getInt8Value(String key) {

      byte value = 0;

      if (key != "") {

         KVSObject kvsObject = dataStore.get(key);

         if (kvsObject != null) {

            if ((kvsObject.typeInfo != TYPE_Info.TYPE_INT8)
                  || (kvsObject.numberElements != 1)) {
               System.err.println("KeyValue store type mismatch for key: "
                     + key);
               return value;
            }

            ByteBuffer bb = ByteBuffer.wrap(kvsObject.byteBuffer);
            value = bb.get();
         }
      }
      return value;
   }

   public synchronized void setInt16Value(String key, short value, DistType varType) {

      KVSObject kvsObject = new KVSObject();

      kvsObject.key = key;
      kvsObject.typeInfo = TYPE_Info.TYPE_INT16;
      kvsObject.numberElements = 1;
      kvsObject.byteBuffer = ByteBuffer.allocate(2).putShort(value).array();

      dataStore.put(key, kvsObject);

      if (varType == DistType.SHARED)
         pubQueue.add(kvsObject);
   }

   public synchronized short getInt16Value(String key) {

      short value = 0;

      if (key != "") {

         KVSObject kvsObject = dataStore.get(key);

         if (kvsObject != null) {

            if ((kvsObject.typeInfo != TYPE_Info.TYPE_INT16)
                  || (kvsObject.numberElements != 1)) {
               System.err.println("KeyValue store type mismatch for key: "
                     + key);
               return value;
            }

            ByteBuffer bb = ByteBuffer.wrap(kvsObject.byteBuffer);
            value = bb.asShortBuffer().get();
         }
      }
      return value;
   }

   public synchronized void setInt32Value(String key, int value, DistType varType) {

      KVSObject kvsObject = new KVSObject();

      kvsObject.key = key;
      kvsObject.typeInfo = TYPE_Info.TYPE_INT32;
      kvsObject.numberElements = 1;
      kvsObject.byteBuffer = ByteBuffer.allocate(4).putInt(value).array();

      dataStore.put(key, kvsObject);

      if (varType == DistType.SHARED)
         pubQueue.add(kvsObject);
   }

   public synchronized int getInt32Value(String key) {

      int value = 0;

      if (key != "") {

         KVSObject kvsObject = dataStore.get(key);

         if (kvsObject != null) {

            if ((kvsObject.typeInfo != TYPE_Info.TYPE_INT32)
                  || (kvsObject.numberElements != 1)) {
               System.err.println("KeyValue store type mismatch for key: "
                     + key);
               return value;
            }

            ByteBuffer bb = ByteBuffer.wrap(kvsObject.byteBuffer);
            value = bb.asIntBuffer().get();
         }
      }
      return value;
   }

   public synchronized void setInt64Value(String key, long value, DistType varType) {

      KVSObject kvsObject = new KVSObject();

      kvsObject.key = key;
      kvsObject.typeInfo = TYPE_Info.TYPE_INT64;
      kvsObject.numberElements = 1;
      kvsObject.byteBuffer = ByteBuffer.allocate(8).putLong(value).array();

      dataStore.put(key, kvsObject);

      if (varType == DistType.SHARED)
         pubQueue.add(kvsObject);
   }

   public synchronized long getInt64Value(String key) {

      long value = 0;

      if (key != "") {

         KVSObject kvsObject = dataStore.get(key);

         if (kvsObject != null) {

            if ((kvsObject.typeInfo != TYPE_Info.TYPE_INT64)
                  || (kvsObject.numberElements != 1)) {
               System.err.println("KeyValue store type mismatch for key: "
                     + key);
               return value;
            }

            ByteBuffer bb = ByteBuffer.wrap(kvsObject.byteBuffer);
            value = bb.asLongBuffer().get();
         }
      }
      return value;
   }

   public synchronized void setFloatValue(String key, float value, DistType varType) {

      KVSObject kvsObject = new KVSObject();

      kvsObject.key = key;
      kvsObject.typeInfo = TYPE_Info.TYPE_FLOAT;
      kvsObject.numberElements = 1;
      kvsObject.byteBuffer = ByteBuffer.allocate(4).putFloat(value).array();

      dataStore.put(key, kvsObject);

      if (varType == DistType.SHARED)
         pubQueue.add(kvsObject);
   }

   public synchronized float getFloatValue(String key) {

      float value = 0;

      if (key != "") {

         KVSObject kvsObject = dataStore.get(key);

         if (kvsObject != null) {

            if ((kvsObject.typeInfo != TYPE_Info.TYPE_FLOAT)
                  || (kvsObject.numberElements != 1)) {
               System.err.println("KeyValue store type mismatch for key: "
                     + key);
               return value;
            }

            ByteBuffer bb = ByteBuffer.wrap(kvsObject.byteBuffer);
            value = bb.asFloatBuffer().get();
         }
      }
      return value;
   }

   public synchronized void setDoubleValue(String key, double value, DistType varType) {

      KVSObject kvsObject = new KVSObject();

      kvsObject.key = key;
      kvsObject.typeInfo = TYPE_Info.TYPE_DOUBLE;
      kvsObject.numberElements = 1;
      kvsObject.byteBuffer = ByteBuffer.allocate(8).putDouble(value).array();

      dataStore.put(key, kvsObject);

      if (varType == DistType.SHARED)
         pubQueue.add(kvsObject);
   }

   public synchronized double getDoubleValue(String key) {

      double value = 0;

      if (key != "") {

         KVSObject kvsObject = dataStore.get(key);

         if (kvsObject != null) {

            if ((kvsObject.typeInfo != TYPE_Info.TYPE_DOUBLE)
                  || (kvsObject.numberElements != 1)) {
               System.err.println("KeyValue store type mismatch for key: "
                     + key);
               return value;
            }

            ByteBuffer bb = ByteBuffer.wrap(kvsObject.byteBuffer);
            value = bb.asDoubleBuffer().get();
         }
      }
      return value;
   }

   public synchronized void setBoolValue(String key, boolean value, DistType varType) {

      byte byteValue = (byte) ((value == false) ? 0 : 1);
      KVSObject kvsObject = new KVSObject();

      kvsObject.key = key;
      kvsObject.typeInfo = TYPE_Info.TYPE_BOOL;
      kvsObject.numberElements = 1;
      kvsObject.byteBuffer = ByteBuffer.allocate(1).put(byteValue).array();

      dataStore.put(key, kvsObject);

      if (varType == DistType.SHARED)
         pubQueue.add(kvsObject);
   }

   public synchronized boolean getBoolValue(String key) {

      boolean value = false;
      byte byteValue = 0;

      if (key != "") {

         KVSObject kvsObject = dataStore.get(key);

         if (kvsObject != null) {

            if ((kvsObject.typeInfo != TYPE_Info.TYPE_BOOL)
                  || (kvsObject.numberElements != 1)) {
               System.err.println("KeyValue store type mismatch for key: "
                     + key);
               return value;
            }

            ByteBuffer bb = ByteBuffer.wrap(kvsObject.byteBuffer);
            byteValue = bb.get();
            value = (byteValue == 0) ? false : true;
         }
      }
      return value;
   }

   public synchronized void setStringValue(String key, String value, DistType varType) {

      byte[] byteBuffer = null;
      KVSObject kvsObject = new KVSObject();

      if (value.length() < 1)
         return;

      try {
         byteBuffer = value.getBytes("UTF-8");
      } catch (UnsupportedEncodingException e) {
      }

      kvsObject.key = key;
      kvsObject.typeInfo = TYPE_Info.TYPE_STRING;
      kvsObject.numberElements = (short) value.length();
      kvsObject.byteBuffer = byteBuffer;

      dataStore.put(key, kvsObject);

      if (varType == DistType.SHARED)
         pubQueue.add(kvsObject);
   }

   public synchronized String getStringValue(String key) {

      String value = "";

      if (key != "") {

         KVSObject kvsObject = dataStore.get(key);

         if (kvsObject != null) {

            if (kvsObject.typeInfo != TYPE_Info.TYPE_STRING) {
               System.err.println("KeyValue store type mismatch for key: "
                     + key);
               return value;
            }

            byte[] byteBuffer = kvsObject.byteBuffer;
            try {
               value = new String(byteBuffer, "UTF-8");
            } catch (UnsupportedEncodingException e) {
            }
         }
      }
      return value;
   }

   public synchronized void setByteBufferValue(String key, byte[] value, DistType varType) {

      KVSObject kvsObject = new KVSObject();

      if (value.length < 1)
         return;

      kvsObject.key = key;
      kvsObject.typeInfo = TYPE_Info.TYPE_BYTES;
      kvsObject.numberElements = (short) value.length;
      kvsObject.byteBuffer = value;

      dataStore.put(key, kvsObject);

      if (varType == DistType.SHARED)
         pubQueue.add(kvsObject);
   }

   public synchronized byte[] getByteBufferValue(String key) {

      byte[] value = null;

      if (key != "") {

         KVSObject kvsObject = dataStore.get(key);

         if (kvsObject != null) {

            if (kvsObject.typeInfo != TYPE_Info.TYPE_BYTES) {
               System.err.println("KeyValue store type mismatch for key: "
                     + key);
               return value;
            }

            value = kvsObject.byteBuffer;
         }
      }
      return value;
   }

   public synchronized void setInt8Array(String key, byte[] array, DistType varType) {

      KVSObject kvsObject = new KVSObject();
      ByteBuffer arrayBuffer = ByteBuffer.allocate(array.length * 1);

      if (array.length < 1)
         return;

      kvsObject.key = key;
      kvsObject.typeInfo = TYPE_Info.TYPE_INT8;
      kvsObject.numberElements = (short) array.length;
      for (int i = 0; i < kvsObject.numberElements; i++)
         arrayBuffer.put(array[i]);
      kvsObject.byteBuffer = arrayBuffer.array();

      dataStore.put(key, kvsObject);

      if (varType == DistType.SHARED)
         pubQueue.add(kvsObject);
   }

   public synchronized byte[] getInt8Array(String key) {

      byte[] value = null;

      if (key != "") {

         KVSObject kvsObject = dataStore.get(key);

         if (kvsObject != null) {

            if (kvsObject.typeInfo != TYPE_Info.TYPE_INT8) {
               System.err.println("KeyValue store type mismatch for key: "
                     + key);
               return value;
            }

            value = new byte[kvsObject.numberElements];

            for (int i = 0; i < kvsObject.numberElements; i++) {

               ByteBuffer bb = ByteBuffer.wrap(kvsObject.byteBuffer, i * 1, 1);
               value[i] = bb.get();
            }
         }
      }
      return value;
   }

   public synchronized void setInt16Array(String key, short[] array, DistType varType) {

      KVSObject kvsObject = new KVSObject();
      ByteBuffer arrayBuffer = ByteBuffer.allocate(array.length * 2);

      if (array.length < 1)
         return;

      kvsObject.key = key;
      kvsObject.typeInfo = TYPE_Info.TYPE_INT16;
      kvsObject.numberElements = (short) array.length;
      for (int i = 0; i < kvsObject.numberElements; i++)
         arrayBuffer.putShort(array[i]);
      kvsObject.byteBuffer = arrayBuffer.array();

      dataStore.put(key, kvsObject);

      if (varType == DistType.SHARED)
         pubQueue.add(kvsObject);
   }

   public synchronized short[] getInt16Array(String key) {

      short[] value = null;

      if (key != "") {

         KVSObject kvsObject = dataStore.get(key);

         if (kvsObject != null) {

            if (kvsObject.typeInfo != TYPE_Info.TYPE_INT16) {
               System.err.println("KeyValue store type mismatch for key: "
                     + key);
               return value;
            }

            value = new short[kvsObject.numberElements];

            for (int i = 0; i < kvsObject.numberElements; i++) {

               ByteBuffer bb = ByteBuffer.wrap(kvsObject.byteBuffer, i * 2, 2);
               value[i] = bb.getShort();
            }
         }
      }
      return value;
   }

   public synchronized void setInt32Array(String key, int[] array, DistType varType) {

      KVSObject kvsObject = new KVSObject();
      ByteBuffer arrayBuffer = ByteBuffer.allocate(array.length * 4);

      if (array.length < 1)
         return;

      kvsObject.key = key;
      kvsObject.typeInfo = TYPE_Info.TYPE_INT32;
      kvsObject.numberElements = (short) array.length;
      for (int i = 0; i < kvsObject.numberElements; i++)
         arrayBuffer.putInt(array[i]);
      kvsObject.byteBuffer = arrayBuffer.array();

      dataStore.put(key, kvsObject);

      if (varType == DistType.SHARED)
         pubQueue.add(kvsObject);
   }

   public synchronized int[] getInt32Array(String key) {

      int[] value = null;

      if (key != "") {

         KVSObject kvsObject = dataStore.get(key);

         if (kvsObject != null) {

            if (kvsObject.typeInfo != TYPE_Info.TYPE_INT32) {
               System.err.println("KeyValue store type mismatch for key: "
                     + key);
               return value;
            }

            value = new int[kvsObject.numberElements];

            for (int i = 0; i < kvsObject.numberElements; i++) {

               ByteBuffer bb = ByteBuffer.wrap(kvsObject.byteBuffer, i * 4, 4);
               value[i] = bb.getInt();
            }
         }
      }
      return value;
   }

   public synchronized void setInt64Array(String key, long[] array, DistType varType) {

      KVSObject kvsObject = new KVSObject();
      ByteBuffer arrayBuffer = ByteBuffer.allocate(array.length * 8);

      if (array.length < 1)
         return;

      kvsObject.key = key;
      kvsObject.typeInfo = TYPE_Info.TYPE_INT64;
      kvsObject.numberElements = (short) array.length;
      for (int i = 0; i < kvsObject.numberElements; i++)
         arrayBuffer.putLong(array[i]);
      kvsObject.byteBuffer = arrayBuffer.array();

      dataStore.put(key, kvsObject);

      if (varType == DistType.SHARED)
         pubQueue.add(kvsObject);
   }

   public synchronized long[] getInt64Array(String key) {

      long[] value = null;

      if (key != "") {

         KVSObject kvsObject = dataStore.get(key);

         if (kvsObject != null) {

            if (kvsObject.typeInfo != TYPE_Info.TYPE_INT64) {
               System.err.println("KeyValue store type mismatch for key: "
                     + key);
               return value;
            }

            value = new long[kvsObject.numberElements];

            for (int i = 0; i < kvsObject.numberElements; i++) {

               ByteBuffer bb = ByteBuffer.wrap(kvsObject.byteBuffer, i * 8, 8);
               value[i] = bb.getLong();
            }
         }
      }
      return value;
   }

   public synchronized void setFloatArray(String key, float[] array, DistType varType) {

      KVSObject kvsObject = new KVSObject();
      ByteBuffer arrayBuffer = ByteBuffer.allocate(array.length * 4);

      if (array.length < 1)
         return;

      kvsObject.key = key;
      kvsObject.typeInfo = TYPE_Info.TYPE_FLOAT;
      kvsObject.numberElements = (short) array.length;
      for (int i = 0; i < kvsObject.numberElements; i++)
         arrayBuffer.putFloat(array[i]);
      kvsObject.byteBuffer = arrayBuffer.array();

      dataStore.put(key, kvsObject);

      if (varType == DistType.SHARED)
         pubQueue.add(kvsObject);
   }

   public synchronized float[] getFloatArray(String key) {

      float[] value = null;

      if (key != "") {

         KVSObject kvsObject = dataStore.get(key);

         if (kvsObject != null) {

            if (kvsObject.typeInfo != TYPE_Info.TYPE_FLOAT) {
               System.err.println("KeyValue store type mismatch for key: "
                     + key);
               return value;
            }

            value = new float[kvsObject.numberElements];

            for (int i = 0; i < kvsObject.numberElements; i++) {

               ByteBuffer bb = ByteBuffer.wrap(kvsObject.byteBuffer, i * 4, 4);
               value[i] = bb.getFloat();
            }
         }
      }
      return value;
   }

   public synchronized void setDoubleArray(String key, double[] array, DistType varType) {

      KVSObject kvsObject = new KVSObject();
      ByteBuffer arrayBuffer = ByteBuffer.allocate(array.length * 8);

      if (array.length < 1)
         return;

      kvsObject.key = key;
      kvsObject.typeInfo = TYPE_Info.TYPE_DOUBLE;
      kvsObject.numberElements = (short) array.length;
      for (int i = 0; i < kvsObject.numberElements; i++)
         arrayBuffer.putDouble(array[i]);
      kvsObject.byteBuffer = arrayBuffer.array();

      dataStore.put(key, kvsObject);

      if (varType == DistType.SHARED)
         pubQueue.add(kvsObject);
   }

   public synchronized double[] getDoubleArray(String key) {

      double[] value = null;

      if (key != "") {

         KVSObject kvsObject = dataStore.get(key);

         if (kvsObject != null) {

            if (kvsObject.typeInfo != TYPE_Info.TYPE_DOUBLE) {
               System.err.println("KeyValue store type mismatch for key: "
                     + key);
               return value;
            }

            value = new double[kvsObject.numberElements];

            for (int i = 0; i < kvsObject.numberElements; i++) {

               ByteBuffer bb = ByteBuffer.wrap(kvsObject.byteBuffer, i * 8, 8);
               value[i] = bb.getDouble();
            }
         }
      }
      return value;
   }

   public synchronized void setBoolArray(String key, boolean[] array, DistType varType) {

      KVSObject kvsObject = new KVSObject();
      ByteBuffer arrayBuffer = ByteBuffer.allocate(array.length * 1);

      if (array.length < 1)
         return;

      kvsObject.key = key;
      kvsObject.typeInfo = TYPE_Info.TYPE_BOOL;
      kvsObject.numberElements = (short) array.length;
      for (int i = 0; i < kvsObject.numberElements; i++) {
         byte byteValue = (byte) ((array[i] == false) ? 0 : 1);
         arrayBuffer.put(byteValue);
      }
      kvsObject.byteBuffer = arrayBuffer.array();

      dataStore.put(key, kvsObject);

      if (varType == DistType.SHARED)
         pubQueue.add(kvsObject);
   }

   public synchronized boolean[] getBoolArray(String key) {

      boolean[] value = null;

      if (key != "") {

         KVSObject kvsObject = dataStore.get(key);

         if (kvsObject != null) {

            if (kvsObject.typeInfo != TYPE_Info.TYPE_BOOL) {
               System.err.println("KeyValue store type mismatch for key: "
                     + key);
               return value;
            }

            value = new boolean[kvsObject.numberElements];

            for (int i = 0; i < kvsObject.numberElements; i++) {

               ByteBuffer bb = ByteBuffer.wrap(kvsObject.byteBuffer, i * 1, 1);
               value[i] = (bb.get() == 0) ? false : true;
            }
         }
      }
      return value;
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

      DDSKVStore.TransactionTypeSupport ts;

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
      ts = new DDSKVStore.TransactionTypeSupport();
      ts.register_type(dp, "DDSKVStore::Transaction");

      topic = dp.create_topic("Transaction", "DDSKVStore::Transaction",
            tQos.value, null, DDS.STATUS_MASK_NONE.value);

      // Create Datareader
      dataReader = subscriber.create_datareader(topic, drQos.value, null,
            DDS.STATUS_MASK_NONE.value);
      transactionDataReader = DDSKVStore.TransactionDataReaderHelper
            .narrow(dataReader);

      if (dataReader == null)
         System.err.println("ERROR: DDS Connection failed");

      // Create Datawriter
      transactionDataWriter = DDSKVStore.TransactionDataWriterHelper
            .narrow(publisher.create_datawriter(topic, dwQos.value, null,
                  DDS.STATUS_MASK_NONE.value));

      if (transactionDataWriter == null)
         System.err.println("ERROR: DDS Connection failed");

      // Create Readcondition
      readCondition = dataReader.create_readcondition(
            DDS.ANY_SAMPLE_STATE.value, DDS.ANY_VIEW_STATE.value,
            DDS.ALIVE_INSTANCE_STATE.value);
   }

   public void run() {

      DDSKVStore.TransactionSeqHolder transactionSeq = new DDSKVStore.TransactionSeqHolder();
      DDSKVStore.Transaction rcvTransaction;
      DDS.SampleInfoSeqHolder infoSeq = new DDS.SampleInfoSeqHolder();

      init();

      while (!shutdownRequested) {

         transactionDataReader.take_w_condition(transactionSeq, infoSeq,
               DDS.LENGTH_UNLIMITED.value, readCondition);

         if (infoSeq.value != null && infoSeq.value.length > 0) {

            for (int i = 0; i < transactionSeq.value.length; i++) {

               if (infoSeq.value[i].valid_data) {

                  rcvTransaction = transactionSeq.value[i];

                  // Only update the dataStore from transactions originating
                  // from other kvstore publishers
                  if ((rcvTransaction.action == DBASE_Action.DBASE_SET)
                        && rcvTransaction.publisherID != publisherID) {

                     for (int j = 0; j < rcvTransaction.keyValueSet.length; j++) {

                        KVSObject kvsObject = new KVSObject();

                        kvsObject.key = rcvTransaction.keyValueSet[j].key;
                        kvsObject.typeInfo = rcvTransaction.keyValueSet[j].typeInfo;
                        kvsObject.numberElements = rcvTransaction.keyValueSet[j].numberElements;
                        kvsObject.byteBuffer = rcvTransaction.keyValueSet[j].byteBuffer;

                        // Perform byte swapping (if required)
                        if (rcvTransaction.systemByteOrder != systemByteOrder) {

                           switch (kvsObject.typeInfo.value()) {

                           case TYPE_Info._TYPE_INT16:

                              if (kvsObject.numberElements == 1)
                                 kvsObject.byteBuffer = ByteSwapper
                                       .swapTwoBytes(kvsObject.byteBuffer);
                              else
                                 kvsObject.byteBuffer = ByteSwapper
                                       .swapTwoByteArray(kvsObject.byteBuffer,
                                             kvsObject.numberElements);
                              break;

                           case TYPE_Info._TYPE_INT32:
                           case TYPE_Info._TYPE_FLOAT:

                              if (kvsObject.numberElements == 1)
                                 kvsObject.byteBuffer = ByteSwapper
                                       .swapFourBytes(kvsObject.byteBuffer);
                              else
                                 kvsObject.byteBuffer = ByteSwapper
                                       .swapFourByteArray(kvsObject.byteBuffer,
                                             kvsObject.numberElements);
                              break;

                           case TYPE_Info._TYPE_INT64:
                           case TYPE_Info._TYPE_DOUBLE:

                              if (kvsObject.numberElements == 1)
                                 kvsObject.byteBuffer = ByteSwapper
                                       .swapEightBytes(kvsObject.byteBuffer);
                              else
                                 kvsObject.byteBuffer = ByteSwapper
                                       .swapEightByteArray(
                                             kvsObject.byteBuffer,
                                             kvsObject.numberElements);
                              break;

                           default:
                              break;

                           }
                        }
                        synchronized(this) {
                           dataStore.put(kvsObject.key, kvsObject);
                        }
                     }
                  }
               }
            }
         }

         transactionDataReader.return_loan(transactionSeq, infoSeq);

         publishPending();
         
         try {
            Thread.sleep(30);
         } catch (InterruptedException ex) { }
      }

      threadHasComplete = true;
   }

   private void publishPending() {

      int size = pubQueue.size();

      if (size > 0) {

         if (size > DDSKVStore.MAX_KVS_SIZE.value)
            size = DDSKVStore.MAX_KVS_SIZE.value;

         pubTransaction.publisherID = publisherID;
         pubTransaction.timeStamp = System.currentTimeMillis();
         pubTransaction.action = DBASE_Action.DBASE_SET;
         pubTransaction.systemByteOrder = systemByteOrder;
         pubTransaction.keyValueSet = new KeyValue[size];

         for (int i = 0; i < size; i++) {

            KVSObject kvsObject = pubQueue.poll();

            if (kvsObject != null) {

               pubTransaction.keyValueSet[i] = new KeyValue();
               pubTransaction.keyValueSet[i].key = kvsObject.key;
               pubTransaction.keyValueSet[i].typeInfo = kvsObject.typeInfo;
               pubTransaction.keyValueSet[i].numberElements = kvsObject.numberElements;
               pubTransaction.keyValueSet[i].byteBuffer = kvsObject.byteBuffer;
            }
         }

         transactionDataWriter.write(pubTransaction, DDS.HANDLE_NIL.value);
      }
   }

   private DDS.DataReader dataReader;
   private DDS.ReadCondition readCondition;

   private DDSKVStore.TransactionDataReader transactionDataReader;
   private DDSKVStore.TransactionDataWriter transactionDataWriter;
   private DDSKVStore.Transaction pubTransaction;

   private boolean shutdownRequested;
   private boolean threadHasComplete;
   private String partition;
   private int publisherID;

   private DDSKVStore.ARCH_Info systemByteOrder;
   private Queue<KVSObject> pubQueue;
   private Map<String, KVSObject> dataStore;
}
