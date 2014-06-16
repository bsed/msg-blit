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

#include "KVStore_RT.hpp"
#include <sys/time.h>
#include <ByteSwapper.hpp>

KVStore::KVStore(std::string partition, int publisherID) {

   systemByteOrder = DDSKVStore::ARCH_LITTLE_ENDIAN;
   this->partition = partition;
   this->publisherID = publisherID;
}

void KVStore::setInt8Value(std::string key, int8_t value, DistType varType) {

   KVSObject kvsObject;

   kvsObject.key = key;
   kvsObject.typeInfo = DDSKVStore::TYPE_INT8;
   kvsObject.numberElements = 1;
   kvsObject.byteBuffer.resize(sizeof(value));
   memcpy(&kvsObject.byteBuffer[0], &value, sizeof(value));

   dataStore[key] = kvsObject;

   if (varType == SHARED)
      pubQueue.push(kvsObject);
}

int8_t KVStore::getInt8Value(std::string key) {

   int8_t value = 0;

   if (!key.empty()) {

      if (dataStore.count(key) > 0) {

         KVSObject kvsObject = dataStore[key];

         if ((kvsObject.typeInfo != DDSKVStore::TYPE_INT8)
               || (kvsObject.numberElements != 1)) {
            std::cerr << "KeyValue store type mismatch for key: " << key
                  << std::endl;
            return value;
         }

         memcpy(&value, &kvsObject.byteBuffer[0], sizeof(value));
      }
   }

   return value;
}

void KVStore::setInt16Value(std::string key, int16_t value, DistType varType) {

   KVSObject kvsObject;

   kvsObject.key = key;
   kvsObject.typeInfo = DDSKVStore::TYPE_INT16;
   kvsObject.numberElements = 1;
   kvsObject.byteBuffer.resize(sizeof(value));
   memcpy(&kvsObject.byteBuffer[0], &value, sizeof(value));

   dataStore[key] = kvsObject;

   if (varType == SHARED)
      pubQueue.push(kvsObject);
}

int16_t KVStore::getInt16Value(std::string key) {

   int16_t value = 0;

   if (!key.empty()) {

      if (dataStore.count(key) > 0) {

         KVSObject kvsObject = dataStore[key];

         if ((kvsObject.typeInfo != DDSKVStore::TYPE_INT16)
               || (kvsObject.numberElements != 1)) {
            std::cerr << "KeyValue store type mismatch for key: " << key
                  << std::endl;
            return value;
         }

         memcpy(&value, &kvsObject.byteBuffer[0], sizeof(value));
      }
   }

   return value;
}

void KVStore::setInt32Value(std::string key, int32_t value, DistType varType) {

   KVSObject kvsObject;

   kvsObject.key = key;
   kvsObject.typeInfo = DDSKVStore::TYPE_INT32;
   kvsObject.numberElements = 1;
   kvsObject.byteBuffer.resize(sizeof(value));
   memcpy(&kvsObject.byteBuffer[0], &value, sizeof(value));

   dataStore[key] = kvsObject;

   if (varType == SHARED)
      pubQueue.push(kvsObject);
}

int32_t KVStore::getInt32Value(std::string key) {

   int32_t value = 0;

   if (!key.empty()) {

      if (dataStore.count(key) > 0) {

         KVSObject kvsObject = dataStore[key];

         if ((kvsObject.typeInfo != DDSKVStore::TYPE_INT32)
               || (kvsObject.numberElements != 1)) {
            std::cerr << "KeyValue store type mismatch for key: " << key
                  << std::endl;
            return value;
         }

         memcpy(&value, &kvsObject.byteBuffer[0], sizeof(value));
      }
   }

   return value;
}

void KVStore::setInt64Value(std::string key, int64_t value, DistType varType) {

   KVSObject kvsObject;

   kvsObject.key = key;
   kvsObject.typeInfo = DDSKVStore::TYPE_INT64;
   kvsObject.numberElements = 1;
   kvsObject.byteBuffer.resize(sizeof(value));
   memcpy(&kvsObject.byteBuffer[0], &value, sizeof(value));

   dataStore[key] = kvsObject;

   if (varType == SHARED)
      pubQueue.push(kvsObject);
}

int64_t KVStore::getInt64Value(std::string key) {

   int64_t value = 0;

   if (!key.empty()) {

      if (dataStore.count(key) > 0) {

         KVSObject kvsObject = dataStore[key];

         if ((kvsObject.typeInfo != DDSKVStore::TYPE_INT64)
               || (kvsObject.numberElements != 1)) {
            std::cerr << "KeyValue store type mismatch for key: " << key
                  << std::endl;
            return value;
         }

         memcpy(&value, &kvsObject.byteBuffer[0], sizeof(value));
      }
   }

   return value;
}

void KVStore::setFloatValue(std::string key, float value, DistType varType) {

   KVSObject kvsObject;

   kvsObject.key = key;
   kvsObject.typeInfo = DDSKVStore::TYPE_FLOAT;
   kvsObject.numberElements = 1;
   kvsObject.byteBuffer.resize(sizeof(value));
   memcpy(&kvsObject.byteBuffer[0], &value, sizeof(value));

   dataStore[key] = kvsObject;

   if (varType == SHARED)
      pubQueue.push(kvsObject);
}

float KVStore::getFloatValue(std::string key) {

   float value = 0;

   if (!key.empty()) {

      if (dataStore.count(key) > 0) {

         KVSObject kvsObject = dataStore[key];

         if ((kvsObject.typeInfo != DDSKVStore::TYPE_FLOAT)
               || (kvsObject.numberElements != 1)) {
            std::cerr << "KeyValue store type mismatch for key: " << key
                  << std::endl;
            return value;
         }

         memcpy(&value, &kvsObject.byteBuffer[0], sizeof(value));
      }
   }

   return value;
}

void KVStore::setDoubleValue(std::string key, double value, DistType varType) {

   KVSObject kvsObject;

   kvsObject.key = key;
   kvsObject.typeInfo = DDSKVStore::TYPE_DOUBLE;
   kvsObject.numberElements = 1;
   kvsObject.byteBuffer.resize(sizeof(value));
   memcpy(&kvsObject.byteBuffer[0], &value, sizeof(value));

   dataStore[key] = kvsObject;

   if (varType == SHARED)
      pubQueue.push(kvsObject);
}

double KVStore::getDoubleValue(std::string key) {

   double value = 0;

   if (!key.empty()) {

      if (dataStore.count(key) > 0) {

         KVSObject kvsObject = dataStore[key];

         if ((kvsObject.typeInfo != DDSKVStore::TYPE_DOUBLE)
               || (kvsObject.numberElements != 1)) {
            std::cerr << "KeyValue store type mismatch for key: " << key
                  << std::endl;
            return value;
         }

         memcpy(&value, &kvsObject.byteBuffer[0], sizeof(value));
      }
   }

   return value;
}

void KVStore::setBoolValue(std::string key, bool value, DistType varType) {

   uint8_t byteValue = (uint8_t)((value == false) ? 0 : 1);
   KVSObject kvsObject;

   kvsObject.key = key;
   kvsObject.typeInfo = DDSKVStore::TYPE_BOOL;
   kvsObject.numberElements = 1;
   kvsObject.byteBuffer.resize(sizeof(byteValue));
   memcpy(&kvsObject.byteBuffer[0], &byteValue, sizeof(byteValue));

   dataStore[key] = kvsObject;

   if (varType == SHARED)
      pubQueue.push(kvsObject);
}

bool KVStore::getBoolValue(std::string key) {

   bool value = false;
   uint8_t byteValue = 0;

   if (!key.empty()) {

      if (dataStore.count(key) > 0) {

         KVSObject kvsObject = dataStore[key];

         if ((kvsObject.typeInfo != DDSKVStore::TYPE_BOOL)
               || (kvsObject.numberElements != 1)) {
            std::cerr << "KeyValue store type mismatch for key: " << key
                  << std::endl;
            return value;
         }

         memcpy(&byteValue, &kvsObject.byteBuffer[0], sizeof(byteValue));
         value = (byteValue == 0) ? false : true;
      }
   }

   return value;
}

void KVStore::setStringValue(std::string key, std::string value,
      DistType varType) {

   KVSObject kvsObject;

   if (value.length() < 1)
      return;

   kvsObject.key = key;
   kvsObject.typeInfo = DDSKVStore::TYPE_STRING;
   kvsObject.numberElements = value.length();
   kvsObject.byteBuffer.resize(value.length());
   memcpy(&kvsObject.byteBuffer[0], value.c_str(), value.length());

   dataStore[key] = kvsObject;

   if (varType == SHARED)
      pubQueue.push(kvsObject);
}

std::string KVStore::getStringValue(std::string key) {

   std::string value = "";
   char* c_str_value;

   if (!key.empty()) {

      if (dataStore.count(key) > 0) {

         KVSObject kvsObject = dataStore[key];

         if (kvsObject.typeInfo != DDSKVStore::TYPE_STRING) {
            std::cerr << "KeyValue store type mismatch for key: " << key
                  << std::endl;
            return value;
         }

         c_str_value = (char *) &kvsObject.byteBuffer[0];
         c_str_value[kvsObject.numberElements] = '\0';
         value = std::string(c_str_value);
      }
   }

   return value;
}

void KVStore::setByteBufferValue(std::string key, int8_t* byteBuffer,
      int byteBufferLength, DistType varType) {

   KVSObject kvsObject;

   if (byteBufferLength < 1)
      return;

   kvsObject.key = key;
   kvsObject.typeInfo = DDSKVStore::TYPE_BYTES;
   kvsObject.numberElements = byteBufferLength;
   kvsObject.byteBuffer.resize(byteBufferLength);
   memcpy(&kvsObject.byteBuffer[0], byteBuffer, byteBufferLength);

   dataStore[key] = kvsObject;

   if (varType == SHARED)
      pubQueue.push(kvsObject);
}

int32_t KVStore::getByteBufferValue(std::string key, int8_t* byteBuffer) {

   int byteBufferLength = 0;

   if (!key.empty()) {

      if (dataStore.count(key) > 0) {

         KVSObject kvsObject = dataStore[key];

         if (kvsObject.typeInfo != DDSKVStore::TYPE_BYTES) {
            std::cerr << "KeyValue store type mismatch for key: " << key
                  << std::endl;
            return byteBufferLength;
         }

         byteBufferLength = kvsObject.numberElements;
         memcpy(byteBuffer, &kvsObject.byteBuffer[0], kvsObject.numberElements);
      }
   }

   return byteBufferLength;
}

void KVStore::setInt8Array(std::string key, int8_t array[], int arrayLength,
      DistType varType) {

   KVSObject kvsObject;

   if (arrayLength < 1)
      return;

   kvsObject.key = key;
   kvsObject.typeInfo = DDSKVStore::TYPE_INT8;
   kvsObject.numberElements = arrayLength;
   kvsObject.byteBuffer.resize(arrayLength * 1);
   memcpy(&kvsObject.byteBuffer[0], &array[0], arrayLength * 1);

   dataStore[key] = kvsObject;

   if (varType == SHARED)
      pubQueue.push(kvsObject);
}

int32_t KVStore::getInt8Array(std::string key, int8_t array[]) {

   int numberElements = 0;

   if (!key.empty()) {

      if (dataStore.count(key) > 0) {

         KVSObject kvsObject = dataStore[key];

         if (kvsObject.typeInfo != DDSKVStore::TYPE_INT8) {
            std::cerr << "KeyValue store type mismatch for key: " << key
                  << std::endl;
            return numberElements;
         }

         numberElements = kvsObject.numberElements;
         memcpy(&array[0], &kvsObject.byteBuffer[0], (numberElements * 1));
      }
   }

   return numberElements;
}

void KVStore::setInt16Array(std::string key, int16_t array[], int arrayLength,
      DistType varType) {

   KVSObject kvsObject;

   if (arrayLength < 1)
      return;

   kvsObject.key = key;
   kvsObject.typeInfo = DDSKVStore::TYPE_INT16;
   kvsObject.numberElements = arrayLength;
   kvsObject.byteBuffer.resize(arrayLength * 2);
   memcpy(&kvsObject.byteBuffer[0], &array[0], arrayLength * 2);

   dataStore[key] = kvsObject;

   if (varType == SHARED)
      pubQueue.push(kvsObject);
}

int32_t KVStore::getInt16Array(std::string key, int16_t array[]) {

   int numberElements = 0;

   if (!key.empty()) {

      if (dataStore.count(key) > 0) {

         KVSObject kvsObject = dataStore[key];

         if (kvsObject.typeInfo != DDSKVStore::TYPE_INT16) {
            std::cerr << "KeyValue store type mismatch for key: " << key
                  << std::endl;
            return numberElements;
         }

         numberElements = kvsObject.numberElements;
         memcpy(&array[0], &kvsObject.byteBuffer[0], (numberElements * 2));
      }
   }

   return numberElements;
}

void KVStore::setInt32Array(std::string key, int32_t array[], int arrayLength,
      DistType varType) {

   KVSObject kvsObject;

   if (arrayLength < 1)
      return;

   kvsObject.key = key;
   kvsObject.typeInfo = DDSKVStore::TYPE_INT32;
   kvsObject.numberElements = arrayLength;
   kvsObject.byteBuffer.resize(arrayLength * 4);
   memcpy(&kvsObject.byteBuffer[0], &array[0], arrayLength * 4);

   dataStore[key] = kvsObject;

   if (varType == SHARED)
      pubQueue.push(kvsObject);
}

int32_t KVStore::getInt32Array(std::string key, int32_t array[]) {

   int numberElements = 0;

   if (!key.empty()) {

      if (dataStore.count(key) > 0) {

         KVSObject kvsObject = dataStore[key];

         if (kvsObject.typeInfo != DDSKVStore::TYPE_INT32) {
            std::cerr << "KeyValue store type mismatch for key: " << key
                  << std::endl;
            return numberElements;
         }

         numberElements = kvsObject.numberElements;
         memcpy(&array[0], &kvsObject.byteBuffer[0], (numberElements * 4));
      }
   }

   return numberElements;
}

void KVStore::setInt64Array(std::string key, int64_t array[], int arrayLength,
      DistType varType) {

   KVSObject kvsObject;

   if (arrayLength < 1)
      return;

   kvsObject.key = key;
   kvsObject.typeInfo = DDSKVStore::TYPE_INT64;
   kvsObject.numberElements = arrayLength;
   kvsObject.byteBuffer.resize(arrayLength * 8);
   memcpy(&kvsObject.byteBuffer[0], &array[0], arrayLength * 8);

   dataStore[key] = kvsObject;

   if (varType == SHARED)
      pubQueue.push(kvsObject);
}

int32_t KVStore::getInt64Array(std::string key, int64_t array[]) {

   int numberElements = 0;

   if (!key.empty()) {

      if (dataStore.count(key) > 0) {

         KVSObject kvsObject = dataStore[key];

         if (kvsObject.typeInfo != DDSKVStore::TYPE_INT64) {
            std::cerr << "KeyValue store type mismatch for key: " << key
                  << std::endl;
            return numberElements;
         }

         numberElements = kvsObject.numberElements;
         memcpy(&array[0], &kvsObject.byteBuffer[0], (numberElements * 8));
      }
   }

   return numberElements;
}

void KVStore::setFloatArray(std::string key, float array[], int arrayLength,
      DistType varType) {

   KVSObject kvsObject;

   if (arrayLength < 1)
      return;

   kvsObject.key = key;
   kvsObject.typeInfo = DDSKVStore::TYPE_FLOAT;
   kvsObject.numberElements = arrayLength;
   kvsObject.byteBuffer.resize(arrayLength * 4);
   memcpy(&kvsObject.byteBuffer[0], &array[0], arrayLength * 4);

   dataStore[key] = kvsObject;

   if (varType == SHARED)
      pubQueue.push(kvsObject);
}

int32_t KVStore::getFloatArray(std::string key, float array[]) {

   int numberElements = 0;

   if (!key.empty()) {

      if (dataStore.count(key) > 0) {

         KVSObject kvsObject = dataStore[key];

         if (kvsObject.typeInfo != DDSKVStore::TYPE_FLOAT) {
            std::cerr << "KeyValue store type mismatch for key: " << key
                  << std::endl;
            return numberElements;
         }

         numberElements = kvsObject.numberElements;
         memcpy(&array[0], &kvsObject.byteBuffer[0], (numberElements * 4));
      }
   }

   return numberElements;
}

void KVStore::setDoubleArray(std::string key, double array[], int arrayLength,
      DistType varType) {

   KVSObject kvsObject;

   if (arrayLength < 1)
      return;

   kvsObject.key = key;
   kvsObject.typeInfo = DDSKVStore::TYPE_DOUBLE;
   kvsObject.numberElements = arrayLength;
   kvsObject.byteBuffer.resize(arrayLength * 8);
   memcpy(&kvsObject.byteBuffer[0], &array[0], arrayLength * 8);

   dataStore[key] = kvsObject;

   if (varType == SHARED)
      pubQueue.push(kvsObject);
}

int32_t KVStore::getDoubleArray(std::string key, double array[]) {

   int numberElements = 0;

   if (!key.empty()) {

      if (dataStore.count(key) > 0) {

         KVSObject kvsObject = dataStore[key];

         if (kvsObject.typeInfo != DDSKVStore::TYPE_DOUBLE) {
            std::cerr << "KeyValue store type mismatch for key: " << key
                  << std::endl;
            return numberElements;
         }

         numberElements = kvsObject.numberElements;
         memcpy(&array[0], &kvsObject.byteBuffer[0], (numberElements * 8));
      }
   }

   return numberElements;
}

void KVStore::setBoolArray(std::string key, bool array[], int arrayLength,
      DistType varType) {

   KVSObject kvsObject;

   if (arrayLength < 1)
      return;

   kvsObject.key = key;
   kvsObject.typeInfo = DDSKVStore::TYPE_BOOL;
   kvsObject.numberElements = arrayLength;
   kvsObject.byteBuffer.resize(arrayLength * 1);
   memcpy(&kvsObject.byteBuffer[0], &array[0], arrayLength * 1);

   dataStore[key] = kvsObject;

   if (varType == SHARED)
      pubQueue.push(kvsObject);
}

int32_t KVStore::getBoolArray(std::string key, bool array[]) {

   int numberElements = 0;

   if (!key.empty()) {

      if (dataStore.count(key) > 0) {

         KVSObject kvsObject = dataStore[key];

         if (kvsObject.typeInfo != DDSKVStore::TYPE_BOOL) {
            std::cerr << "KeyValue store type mismatch for key: " << key
                  << std::endl;
            return numberElements;
         }

         numberElements = kvsObject.numberElements;
         memcpy(&array[0], &kvsObject.byteBuffer[0], (numberElements * 1));
      }
   }

   return numberElements;
}

void KVStore::init() {

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

   DDSKVStore::TransactionTypeSupport_var ts;

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
   ts = new DDSKVStore::TransactionTypeSupport();
   ts->register_type(dp, "DDSKVStore::Transaction");

   topic = dp->create_topic("Transaction", "DDSKVStore::Transaction", tQos,
         NULL, DDS::STATUS_MASK_NONE);

   // Create Datareader
   dataReader = subscriber->create_datareader(topic, drQos, NULL,
         DDS::STATUS_MASK_NONE);
   transactionDataReader = DDSKVStore::TransactionDataReader::_narrow(
         dataReader);

   if (dataReader == NULL)
      std::cout << "ERROR: DDS Connection failed" << std::endl;

   // Create Datawriter
   transactionDataWriter = DDSKVStore::TransactionDataWriter::_narrow(
         publisher->create_datawriter(topic, dwQos, NULL,
               DDS::STATUS_MASK_NONE));
   if (transactionDataWriter == NULL)
      std::cout << "ERROR: DDS Connection failed" << std::endl;

   // Create Readcondition
   readCondition = dataReader->create_readcondition(DDS::ANY_SAMPLE_STATE,
         DDS::ANY_VIEW_STATE, DDS::ALIVE_INSTANCE_STATE);

}

void KVStore::receiveUpdates() {

   DDSKVStore::TransactionSeq transactionSeq;
   DDSKVStore::Transaction rcvTransaction;
   DDS::SampleInfoSeq infoSeq;
   DDS::ConditionSeq condSeq;

   transactionDataReader->take_w_condition(transactionSeq, infoSeq,
         DDS::LENGTH_UNLIMITED, readCondition);

   if (infoSeq.length() > 0) {

      for (unsigned int i = 0; i < transactionSeq.length(); i++) {

         rcvTransaction = transactionSeq[i];

         // Only update the dataStore from transactions originating
         // from other kvstore publishers
         if ((rcvTransaction.action == DDSKVStore::DBASE_SET)
               && rcvTransaction.publisherID != publisherID) {

            for (unsigned int j = 0; j < rcvTransaction.keyValueSet.length();
                  j++) {

               int bufferLength =
                     rcvTransaction.keyValueSet[j].byteBuffer.length();
               KVSObject kvsObject;

               kvsObject.key = rcvTransaction.keyValueSet[j].key;
               kvsObject.typeInfo = rcvTransaction.keyValueSet[j].typeInfo;
               kvsObject.numberElements =
                     rcvTransaction.keyValueSet[j].numberElements;
               kvsObject.byteBuffer.resize(bufferLength);
               memcpy(&kvsObject.byteBuffer[0],
                     &rcvTransaction.keyValueSet[j].byteBuffer[0],
                     bufferLength);

               // Perform byte swapping (if required)
               if (rcvTransaction.systemByteOrder != systemByteOrder) {

                  int8_t* value_ptr = (int8_t *) &kvsObject.byteBuffer[0];

                  switch (kvsObject.typeInfo) {

                  case DDSKVStore::TYPE_INT16:

                     if (kvsObject.numberElements == 1)
                        ByteSwapper::swapTwoBytes(value_ptr);
                     else
                        ByteSwapper::swapTwoByteArray(value_ptr,
                              kvsObject.numberElements);
                     break;

                  case DDSKVStore::TYPE_INT32:
                  case DDSKVStore::TYPE_FLOAT:

                     if (kvsObject.numberElements == 1)
                        ByteSwapper::swapFourBytes(value_ptr);
                     else
                        ByteSwapper::swapFourByteArray(value_ptr,
                              kvsObject.numberElements);
                     break;

                  case DDSKVStore::TYPE_INT64:
                  case DDSKVStore::TYPE_DOUBLE:
                     if (kvsObject.numberElements == 1)
                        ByteSwapper::swapEightBytes(value_ptr);
                     else
                        ByteSwapper::swapEightByteArray(value_ptr,
                              kvsObject.numberElements);
                     break;

                  default:
                     break;
                  }
               }

               dataStore[kvsObject.key] = kvsObject;
            }
         }
      }
   }

   transactionDataReader->return_loan(transactionSeq, infoSeq);

}

void KVStore::publishPending() {

   int size = pubQueue.size();

   if (size > 0) {

      struct timeval tp;
      gettimeofday(&tp, NULL);

      if (size > DDSKVStore::MAX_KVS_SIZE)
         size = DDSKVStore::MAX_KVS_SIZE;

      pubTransaction.publisherID = publisherID;
      pubTransaction.timeStamp = tp.tv_sec * 1000 + tp.tv_usec / 1000;
      pubTransaction.action = DDSKVStore::DBASE_SET;
      pubTransaction.systemByteOrder = systemByteOrder;
      pubTransaction.keyValueSet.length(size);

      for (int i = 0; i < size; i++) {

         KVSObject kvsObject;

         if (pubQueue.front(kvsObject)) {

            int bufferLength = kvsObject.byteBuffer.size();

            pubTransaction.keyValueSet[i].key = kvsObject.key.c_str();
            pubTransaction.keyValueSet[i].typeInfo = kvsObject.typeInfo;
            pubTransaction.keyValueSet[i].numberElements =
                  kvsObject.numberElements;
            pubTransaction.keyValueSet[i].byteBuffer.length(bufferLength);
            memcpy(&pubTransaction.keyValueSet[i].byteBuffer[0],
                  &kvsObject.byteBuffer[0], bufferLength);
         }
      }

      transactionDataWriter->write(pubTransaction, DDS::HANDLE_NIL);
   }
}
