#include "KVStore.hpp"
#include <boost/thread.hpp>
#include <climits>
#include <stdint.h>
#include <inttypes.h>
#include <float.h>

static bool shutdown = false;

void sigint_handler(int sig) {

   shutdown = true;
}

int main(int argc, char *argv[]) {

   signal(SIGINT, sigint_handler);

   KVStore* database = new KVStore("", 15);

   // ***********  Start of the value declarations ***********

   int8_t byteTest1 = CHAR_MAX;
   int8_t byteTest2 = 105;
   int8_t byteTest3 = CHAR_MIN;

   int16_t shortTest1 = SHRT_MAX;
   int16_t shortTest2 = 24111;
   int16_t shortTest3 = SHRT_MIN;

   int32_t intTest1 = INT_MAX;
   int32_t intTest2 = 7543937;
   int32_t intTest3 = INT_MIN;

   int64_t longTest1 = LONG_LONG_MAX;
   int64_t longTest2 = 299490021;
   int64_t longTest3 = LONG_LONG_MIN;

   float floatTest1 = FLT_MAX;
   float floatTest2 = 76.234911;
   float floatTest3 = FLT_MIN;

   double doubleTest1 = DBL_MAX;
   double doubleTest2 = 973.23521;
   double doubleTest3 = DBL_MIN;

   bool boolTest1 = false;
   bool boolTest2 = true;

   std::string stringTest1 = "this is a test from c++";
   std::string stringTest2 = "";

   int8_t bytesTest1[] = { 7, 8, 9 };
   int8_t bytesTest2[] = { };

   // ***********  Start of the array declarations ***********

   int8_t byteArrayTest1[] = { 87, 89, 91, 93, 95, 97, 99, 101 };
   int8_t byteArrayTest2[] = { };

   int16_t shortArrayTest1[] = { 18, 14, 16, 18, 20, 22 };
   int16_t shortArrayTest2[] = { };

   int32_t intArrayTest1[] = { 500, 1000, 1500, 2000, INT_MIN, INT_MAX };
   int32_t intArrayTest2[] = { };

   int64_t longArrayTest1[] = { LONG_LONG_MAX, 4000, 6000, 8000, 10000,
         LONG_LONG_MIN };
   int64_t longArrayTest2[] = { };

   float floatArrayTest1[] =
         { FLT_MIN, -2312.343, 83411.1423, 9211.41, FLT_MAX };
   float floatArrayTest2[] = { };

   double doubleArrayTest1[] = { DBL_MIN, -7312.52, -3411.1423, 611.41,
         98234.22, DBL_MAX };
   double doubleArrayTest2[] = { };

   bool boolArrayTest1[] =
         { true, true, true, true, false, false, false, false };
   bool boolArrayTest2[] = { };

   database->startReq();

   database->setInt8Value("pig", byteTest2, KVStore::SHARED);
   database->setInt16Value("cat", shortTest2, KVStore::SHARED);
   database->setInt32Value("horse", intTest2, KVStore::SHARED);
   database->setInt64Value("tiger", longTest2, KVStore::SHARED);
   database->setFloatValue("dog", floatTest2, KVStore::SHARED);
   database->setDoubleValue("monkey", doubleTest2, KVStore::SHARED);
   database->setBoolValue("bird", boolTest2, KVStore::SHARED);
   database->setStringValue("fox", stringTest1, KVStore::SHARED);
   database->setByteBufferValue("chicken", &bytesTest1[0], 3, KVStore::SHARED);
   database->setInt8Array("frog", byteArrayTest1, 8, KVStore::SHARED);
   database->setInt16Array("fish", shortArrayTest1, 6, KVStore::SHARED);
   database->setInt32Array("turtle", intArrayTest1, 6, KVStore::SHARED);
   database->setInt64Array("leopard", longArrayTest1, 6, KVStore::SHARED);
   database->setFloatArray("lamb", floatArrayTest1, 5, KVStore::SHARED);
   database->setDoubleArray("mule", doubleArrayTest1, 6, KVStore::SHARED);
   database->setBoolArray("penguin", boolArrayTest1, 8, KVStore::SHARED);

   while (true) {

      int bytesRead, numberElements;
      std::cout << std::endl;

      // ***********  Start of the value tests ***********

      int8_t byteRead;
      byteRead = database->getInt8Value("pig");
      std::cout << (int) byteRead << std::endl;

      short shortRead;
      shortRead = database->getInt16Value("cat");
      std::cout << shortRead << std::endl;

      int intRead;
      intRead = database->getInt32Value("horse");
      std::cout << intRead << std::endl;

      int64_t longRead;
      longRead = database->getInt64Value("tiger");
      std::cout << longRead << std::endl;

      float floatRead;
      floatRead = database->getFloatValue("dog");
      std::cout << floatRead << std::endl;

      double doubleRead;
      doubleRead = database->getDoubleValue("monkey");
      std::cout << doubleRead << std::endl;

      bool boolRead;
      boolRead = database->getBoolValue("bird");
      std::cout << boolRead << std::endl;

      std::string stringRead;
      stringRead = database->getStringValue("fox");
      std::cout << stringRead << stringRead << std::endl;

      int8_t byteBuffer[10];
      bytesRead = database->getByteBufferValue("chicken", &byteBuffer[0]);
      if (bytesRead > 0) {
         std::cout << "Byte Buffer Test - Read " << bytesRead << " bytes"
               << std::endl;
         std::cout << "[";
         for (int i = 0; i < bytesRead; i++)
            std::cout << (int) byteBuffer[i] << " ";
         std::cout << "]" << std::endl;
      }

      // ***********  Start of the array tests ***********
      int8_t byteArray[15];
      numberElements = database->getInt8Array("frog", byteArray);
      if (numberElements > 0) {
         std::cout << "Byte Array Test - Read " << numberElements << " elements"
               << std::endl;
         std::cout << "[";
         for (int i = 0; i < numberElements; i++)
            std::cout << (int) byteArray[i] << " ";
         std::cout << "]" << std::endl;
      }

      int16_t shortArray[10];
      numberElements = database->getInt16Array("fish", shortArray);
      if (numberElements > 0) {
         std::cout << "Short Array Test - Read " << numberElements
               << " elements" << std::endl;
         std::cout << "[";
         for (int i = 0; i < numberElements; i++)
            std::cout << (short) shortArray[i] << " ";
         std::cout << "]" << std::endl;
      }

      int32_t intArray[15];
      numberElements = database->getInt32Array("turtle", intArray);
      if (numberElements > 0) {
         std::cout << "Int Array Test - Read " << numberElements << " elements"
               << std::endl;
         std::cout << "[";
         for (int i = 0; i < numberElements; i++)
            std::cout << (int) intArray[i] << " ";
         std::cout << "]" << std::endl;
      }

      int64_t longArray[15];
      numberElements = database->getInt64Array("leopard", longArray);
      if (numberElements > 0) {
         std::cout << "Long Array Test - Read " << numberElements << " elements"
               << std::endl;
         std::cout << "[";
         for (int i = 0; i < numberElements; i++)
            std::cout << (long long) longArray[i] << " ";
         std::cout << "]" << std::endl;
      }

      float floatArray[15];
      numberElements = database->getFloatArray("lamb", floatArray);
      if (numberElements > 0) {
         std::cout << "Float Array Test - Read " << numberElements
               << " elements" << std::endl;
         std::cout << "[";
         for (int i = 0; i < numberElements; i++)
            std::cout << (float) floatArray[i] << " ";
         std::cout << "]" << std::endl;
      }

      double doubleArray[15];
      numberElements = database->getDoubleArray("mule", doubleArray);
      if (numberElements > 0) {
         std::cout << "Double Array Test - Read " << numberElements
               << " elements" << std::endl;
         std::cout << "[";
         for (int i = 0; i < numberElements; i++)
            std::cout << (double) doubleArray[i] << " ";
         std::cout << "]" << std::endl;
      }

      bool boolArray[15];
      numberElements = database->getBoolArray("penguin", boolArray);
      if (numberElements > 0) {
         std::cout << "Bool Array Test - Read " << numberElements << " elements"
               << std::endl;
         std::cout << "[";
         for (int i = 0; i < numberElements; i++)
            std::cout << (bool) boolArray[i] << " ";
         std::cout << "]" << std::endl;
      }

      if (shutdown) {
         database->shutdownReq();

         while (!database->threadIsComplete())
            boost::this_thread::sleep(boost::posix_time::milliseconds(200));

         abort();
      }

      boost::this_thread::sleep(boost::posix_time::milliseconds(1000));
   }

   return 0;
}
