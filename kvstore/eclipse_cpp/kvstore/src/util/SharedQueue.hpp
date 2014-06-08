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

#ifndef SHARED_QUEUE_H
#define SHARED_QUEUE_H

#include <queue>
#include <boost/thread/mutex.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>

template<typename T> class SharedQueue: private std::queue<T> {

public:

   SharedQueue() { };

   int size() {

      boost::mutex::scoped_lock lock(mutex);
      return std::queue<T>::size();
   }

   void push(T t) {

      boost::mutex::scoped_lock lock(mutex);
      std::queue<T>::push(t);
   }

   bool front(T &t) {

      boost::mutex::scoped_lock lock(mutex);

      if (std::queue<T>::empty())
         return false;

      t = std::queue<T>::front();
      std::queue<T>::pop();
      return true;
   }

private:

   boost::mutex mutex;
};

#endif
