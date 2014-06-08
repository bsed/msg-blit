/*
 *  msg-blit - Implementation of various messaging patterns.
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

#include "ByteSwapper.hpp"
#include <byteswap.h>

void ByteSwapper::swapTwoBytes(int8_t* data) {

   int16_t* buf = (int16_t *) data;
   *buf = __bswap_16(*buf);
}

void ByteSwapper::swapFourBytes(int8_t* data) {

   int32_t* buf = (int32_t *) data;
   *buf = __bswap_32(*buf);
}

void ByteSwapper::swapEightBytes(int8_t* data) {

   int64_t* buf = (int64_t *) data;
   *buf = __bswap_64(*buf);
}

void ByteSwapper::swapTwoByteArray(int8_t* data, int32_t numberElements) {

   for (int i=0; i<numberElements; i++) {

      int16_t* buf = (int16_t *) (data + (i * sizeof(int16_t)));
      *buf = __bswap_16(*buf);
   }
}

void ByteSwapper::swapFourByteArray(int8_t* data, int32_t numberElements) {

   for (int i=0; i<numberElements; i++) {

      int32_t* buf = (int32_t *) (data + (i * sizeof(int32_t)));
      *buf = __bswap_32(*buf);
   }
}

void ByteSwapper::swapEightByteArray(int8_t* data, int32_t numberElements) {

   for (int i=0; i<numberElements; i++) {

      int64_t* buf = (int64_t *) (data + (i * sizeof(int64_t)));
      *buf = __bswap_64(*buf);
   }
}
