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

package orbisoftware.msgblit.eventbus.util;

import java.nio.ByteBuffer;

public class ByteSwapper {

	public static byte[] swapTwoBytes(final byte[] data) {

		ByteBuffer bb = ByteBuffer.wrap(data);
		short value = Short.reverseBytes(bb.asShortBuffer().get());

		return ByteBuffer.allocate(2).putShort(value).array();
	}

	public static byte[] swapFourBytes(byte[] data) {

		ByteBuffer bb = ByteBuffer.wrap(data);
		int value = Integer.reverseBytes(bb.asIntBuffer().get());

		return ByteBuffer.allocate(4).putInt(value).array();
	}

	public static byte[] swapEightBytes(byte[] data) {

		ByteBuffer bb = ByteBuffer.wrap(data);
		long value = Long.reverseBytes(bb.asLongBuffer().get());

		return ByteBuffer.allocate(8).putLong(value).array();
	}

	public static byte[] swapTwoByteArray(byte[] data, int numberElements) {

		int elementSize = 2;
		ByteBuffer swappedBuffer = ByteBuffer.allocate(numberElements
				* elementSize);

		for (int i = 0; i < numberElements; i++) {

			ByteBuffer bb = ByteBuffer.wrap(data, i * elementSize, elementSize);
			short value = Short.reverseBytes(bb.asShortBuffer().get());
			swappedBuffer.put(ByteBuffer.allocate(elementSize).putShort(value)
					.array());
		}

		return swappedBuffer.array();
	}

	public static byte[] swapFourByteArray(byte[] data, int numberElements) {

		int elementSize = 4;
		ByteBuffer swappedBuffer = ByteBuffer.allocate(numberElements
				* elementSize);

		for (int i = 0; i < numberElements; i++) {

			ByteBuffer bb = ByteBuffer.wrap(data, i * elementSize, elementSize);
			int value = Integer.reverseBytes(bb.asIntBuffer().get());
			swappedBuffer.put(ByteBuffer.allocate(elementSize).putInt(value)
					.array());
		}

		return swappedBuffer.array();
	}

	public static byte[] swapEightByteArray(byte[] data, int numberElements) {

		int elementSize = 8;
		ByteBuffer swappedBuffer = ByteBuffer.allocate(numberElements
				* elementSize);

		for (int i = 0; i < numberElements; i++) {

			ByteBuffer bb = ByteBuffer.wrap(data, i * elementSize, elementSize);
			long value = Long.reverseBytes(bb.asLongBuffer().get());
			swappedBuffer.put(ByteBuffer.allocate(elementSize).putLong(value)
					.array());
		}

		return swappedBuffer.array();
	}
}
