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

#ifndef KV_STORE_HPP_
#define KV_STORE_HPP_

#include <ccpp_dds_dcps.h>
#include <DDSKVStore/DDSKVStore.h>
#include <DDSKVStore/ccpp_DDSKVStore.h>
#include <boost/thread.hpp>
#include <KVSObject.hpp>
#include <SharedQueue.hpp>

class KVStore {

public:

   // Used to specify if a variable should be published over network
   // or if it should only be stored locally
   enum DistType {

      SHARED, LOCAL
   };

   // Create object using specified partition and publisherID
   KVStore(std::string partition, int publisherID);

   // Start that must be called prior to any other methods
   void start();

   // Shutdown request
   void shutdownReq();

   // Returns true when thread is complete
   bool threadIsComplete();

   // *********** Start of the single value setter/getter methods ***********

   void setInt8Value(std::string key, int8_t value, DistType varType);
   int8_t getInt8Value(std::string key);

   void setInt16Value(std::string key, int16_t value, DistType varType);
   int16_t getInt16Value(std::string key);

   void setInt32Value(std::string key, int32_t value, DistType varType);
   int32_t getInt32Value(std::string key);

   void setInt64Value(std::string key, int64_t value, DistType varType);
   int64_t getInt64Value(std::string key);

   void setFloatValue(std::string key, float value, DistType varType);
   float getFloatValue(std::string key);

   void setDoubleValue(std::string key, double value, DistType varType);
   double getDoubleValue(std::string key);

   void setBoolValue(std::string key, bool value, DistType varType);
   bool getBoolValue(std::string key);

   void setStringValue(std::string key, std::string value, DistType varType);
   std::string getStringValue(std::string key);

   void setByteBufferValue(std::string key, int8_t* byteBuffer,
         int byteBufferLength, DistType varType);
   int32_t getByteBufferValue(std::string key, int8_t* byteBuffer);

   // ***********  Start of the array setter/getter methods  ***********

   void setInt8Array(std::string key, int8_t array[], int arrayLength,
         DistType varType);
   int32_t getInt8Array(std::string key, int8_t array[]);

   void setInt16Array(std::string key, int16_t array[], int arrayLength,
         DistType varType);
   int32_t getInt16Array(std::string key, int16_t array[]);

   void setInt32Array(std::string key, int32_t array[], int arrayLength,
         DistType varType);
   int32_t getInt32Array(std::string key, int32_t array[]);

   void setInt64Array(std::string key, int64_t array[], int arrayLength,
         DistType varType);
   int32_t getInt64Array(std::string key, int64_t array[]);

   void setFloatArray(std::string key, float array[], int arrayLength,
         DistType varType);
   int32_t getFloatArray(std::string key, float array[]);

   void setDoubleArray(std::string key, double array[], int arrayLength,
         DistType varType);
   int32_t getDoubleArray(std::string key, double array[]);

   void setBoolArray(std::string key, bool array[], int arrayLength,
         DistType varType);
   int32_t getBoolArray(std::string key, bool array[]);

private:

   void init();
   void run();
   void publishPending();

   boost::thread kvStoreThread;

   DDS::DataReader_ptr dataReader;
   DDS::ReadCondition_ptr readCondition;
   DDS::WaitSet_ptr waitSet;

   DDSKVStore::TransactionDataReader_ptr transactionDataReader;
   DDSKVStore::TransactionDataWriter_ptr transactionDataWriter;
   DDSKVStore::Transaction pubTransaction;

   bool shutdownRequested;
   bool threadHasComplete;
   std::string partition;
   int publisherID;

   DDSKVStore::ARCH_Info systemByteOrder;
   SharedQueue<KVSObject> pubQueue;
   std::map<std::string, KVSObject> dataStore;
};

#endif /* KV_STORE_HPP_ */
