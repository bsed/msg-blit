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

#ifndef KVS_OBJECT_HPP_
#define KVS_OBJECT_HPP_

#include <DDSKVStore/DDSKVStore.h>

class KVSObject {

public:

   std::string key;
   DDSKVStore::TYPE_Info typeInfo;
   short numberElements;
   std::vector<int8_t> byteBuffer;
};

#endif /* KVS_OBJECT_HPP_ */
