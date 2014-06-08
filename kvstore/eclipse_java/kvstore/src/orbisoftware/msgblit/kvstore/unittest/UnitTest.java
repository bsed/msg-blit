package orbisoftware.msgblit.kvstore.unittest;

import orbisoftware.msgblit.kvstore.KVStore;
import orbisoftware.msgblit.kvstore.KVStore.DistType;

public class UnitTest {

	public static void main(String args[]) {

		KVStore database = new KVStore("", 25);

		// *********** Start of the value declarations ***********

		byte byteTest1 = 127;
		byte byteTest2 = 103;
		byte byteTest3 = -128;

		short shortTest1 = 32767;
		short shortTest2 = 12345;
		short shortTest3 = -32768;

		int intTest1 = Integer.MAX_VALUE;
		int intTest2 = 3029482;
		int intTest3 = Integer.MIN_VALUE;

		long longTest1 = Long.MAX_VALUE;
		long longTest2 = 1112222333;
		long longTest3 = Long.MIN_VALUE;

		float floatTest1 = Float.MAX_VALUE;
		float floatTest2 = 541.12312f;
		float floatTest3 = Float.MIN_VALUE;

		double doubleTest1 = Double.MAX_VALUE;
		double doubleTest2 = 619887.789;
		double doubleTest3 = Double.MIN_VALUE;

		boolean boolTest1 = false;
		boolean boolTest2 = true;

		String stringTest1 = "this is a test from java";
		String stringTest2 = "";

		byte bytesTest1[] = new byte[] { 5, 6, 7 };
		byte bytesTest2[] = new byte[] {};

		// *********** Start of the array declarations ***********

		byte byteArrayTest1[] = new byte[] { -49, -47, -45, -43, -41 };
		byte byteArrayTest2[] = new byte[] {};

		short shortArrayTest1[] = new short[] { 32, 34, 36, 38, 40 };
		short shortArrayTest2[] = new short[] {};

		int intArrayTest1[] = new int[] { Integer.MAX_VALUE, Integer.MIN_VALUE,
				90, 120, 150, 180, 210, 240, 270, 300 };
		int intArrayTest2[] = new int[] {};

		long longArrayTest1[] = new long[] { -1000, -2000, Long.MIN_VALUE,
				Long.MAX_VALUE, 10000, 5000, 1000 };
		long longArrayTest2[] = new long[] {};

		float floatArrayTest1[] = new float[] { Float.MIN_VALUE, -291.9391f,
				49912.291f, Float.MAX_VALUE };
		float floatArrayTest2[] = new float[] {};

		double doubleArrayTest1[] = new double[] { Double.MAX_VALUE,
				Double.MIN_VALUE, 0.0, Math.PI, Math.E };
		double doubleArrayTest2[] = new double[] {};

		boolean boolArrayTest1[] = new boolean[] { false, false, false, true,
				true, true };
		boolean boolArrayTest2[] = new boolean[] {};

		database.startReq();

		database.setInt8Value("pig", byteTest2, DistType.SHARED);
		database.setInt16Value("cat", shortTest2, DistType.SHARED);
		database.setInt32Value("horse", intTest2, DistType.SHARED);
		database.setInt64Value("tiger", longTest2, DistType.SHARED);
		database.setFloatValue("dog", floatTest2, DistType.SHARED);
		database.setDoubleValue("monkey", doubleTest2, DistType.SHARED);
		database.setBoolValue("bird", boolTest2, DistType.SHARED);
		database.setStringValue("fox", stringTest1, DistType.SHARED);
		database.setByteBufferValue("chicken", bytesTest1, DistType.SHARED);
		database.setInt8Array("frog", byteArrayTest2, DistType.SHARED);
		database.setInt16Array("fish", shortArrayTest1, DistType.SHARED);
		database.setInt32Array("turtle", intArrayTest1, DistType.SHARED);
		database.setInt64Array("leopard", longArrayTest1, DistType.SHARED);
		database.setFloatArray("lamb", floatArrayTest1, DistType.SHARED);
		database.setDoubleArray("mule", doubleArrayTest1, DistType.SHARED);
		database.setBoolArray("penguin", boolArrayTest1, DistType.SHARED);

		while (true) {

			System.out.println("");

			// *********** Start of the value tests ***********

			byte byteRead;
			byteRead = database.getInt8Value("pig");
			System.out.println(byteRead);

			short shortRead;
			shortRead = database.getInt16Value("cat");
			System.out.println(shortRead);

			int intRead;
			intRead = database.getInt32Value("horse");
			System.out.println(intRead);

			long longRead;
			longRead = database.getInt64Value("tiger");
			System.out.println(longRead);

			float floatRead;
			floatRead = database.getFloatValue("dog");
			System.out.println(floatRead);

			double doubleRead;
			doubleRead = database.getDoubleValue("monkey");
			System.out.println(doubleRead);

			boolean boolRead;
			boolRead = database.getBoolValue("bird");
			System.out.println(boolRead);

			String stringRead;
			stringRead = database.getStringValue("fox");
			System.out.println(stringRead + stringRead);

			byte bytesRead[];
			bytesRead = database.getByteBufferValue("chicken");
			System.out.println("Byte Buffer Test - Read " + bytesRead.length
					+ " bytes");
			System.out.print("[");
			for (int i = 0; i < bytesRead.length; i++)
				System.out.print(bytesRead[i] + " ");
			System.out.println("]");

			// *********** Start of the array tests ***********

			byte byteArrayRead[];
			byteArrayRead = database.getInt8Array("frog");
			if (byteArrayRead != null) {
				System.out.println("Byte Array Test - Read "
						+ byteArrayRead.length + " elements");
				System.out.print("[");
				for (int i = 0; i < byteArrayRead.length; i++)
					System.out.print(byteArrayRead[i] + " ");
				System.out.println("]");
			}

			short shortArrayRead[];
			shortArrayRead = database.getInt16Array("fish");
			if (shortArrayRead != null) {
				System.out.println("Short Array Test - Read "
						+ shortArrayRead.length + " elements");
				System.out.print("[");
				for (int i = 0; i < shortArrayRead.length; i++)
					System.out.print(shortArrayRead[i] + " ");
				System.out.println("]");
			}

			int intArrayRead[];
			intArrayRead = database.getInt32Array("turtle");
			if (intArrayRead != null) {
				System.out.println("Int Array Test - Read "
						+ intArrayRead.length + " elements");
				System.out.print("[");
				for (int i = 0; i < intArrayRead.length; i++)
					System.out.print(intArrayRead[i] + " ");
				System.out.println("]");
			}

			long longArrayRead[];
			longArrayRead = database.getInt64Array("leopard");
			if (longArrayRead != null) {
				System.out.println("Long Array Test - Read "
						+ longArrayRead.length + " elements");
				System.out.print("[");
				for (int i = 0; i < longArrayRead.length; i++)
					System.out.print(longArrayRead[i] + " ");
				System.out.println("]");
			}

			float floatArrayRead[];
			floatArrayRead = database.getFloatArray("lamb");
			if (floatArrayRead != null) {
				System.out.println("Float Array Test - Read "
						+ floatArrayRead.length + " elements");
				System.out.print("[");
				for (int i = 0; i < floatArrayRead.length; i++)
					System.out.print(floatArrayRead[i] + " ");
				System.out.println("]");
			}

			double doubleArrayRead[];
			doubleArrayRead = database.getDoubleArray("mule");
			if (doubleArrayRead != null) {
				System.out.println("Double Array Test - Read "
						+ doubleArrayRead.length + " elements");
				System.out.print("[");
				for (int i = 0; i < doubleArrayRead.length; i++)
					System.out.print(doubleArrayRead[i] + " ");
				System.out.println("]");
			}

			boolean boolArrayRead[];
			boolArrayRead = database.getBoolArray("penguin");
			if (boolArrayRead != null) {
				System.out.println("Bool Array Test - Read "
						+ boolArrayRead.length + " elements");
				System.out.print("[");
				for (int i = 0; i < boolArrayRead.length; i++)
					System.out.print(boolArrayRead[i] + " ");
				System.out.println("]");
			}

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
