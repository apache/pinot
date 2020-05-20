package org.apache.pinot.core.io.util;

import java.nio.ByteOrder;
import java.util.Random;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PinotDataBitSetV2Test {

  @Test
  public void testBit2Encoded() throws Exception {
    int cardinality = 3;
    int rows = 10000;
    int[] forwardIndex = new int[rows];
    Random random = new Random();

    for (int i = 0; i < rows; i++) {
      forwardIndex[i] = random.nextInt(cardinality);
    }

    int numBitsPerValue = PinotDataBitSet.getNumBitsPerValue(cardinality - 1);
    int bitPackedBufferSize = (rows * numBitsPerValue + Byte.SIZE - 1) / Byte.SIZE;
    PinotDataBitSetV2 bitSet = getEmptyBitSet(bitPackedBufferSize, numBitsPerValue);

    Assert.assertEquals(2, numBitsPerValue);
    Assert.assertTrue(bitSet instanceof PinotDataBitSetV2.Bit2Encoded);

    for (int i = 0; i < rows; i++) {
      bitSet.writeInt(i, forwardIndex[i]);
    }

    // single read
    for (int i = 0; i < rows; i++) {
      int unpacked = bitSet.readInt(i);
      Assert.assertEquals(forwardIndex[i], unpacked);
    }

    // for each batch:
    // 3 aligned reads at 4-byte boundary to unpack 16 integers after each read -- 48 integers unpacked
    // followed by reading the next byte to unpack 2 integer from first 4 bits
    int batchLength = 50;
    int[] unpacked = new int[batchLength];
    int startDocId = 0;
    for (startDocId = 0; startDocId < rows; startDocId += 50) {
      bitSet.readInt(startDocId, batchLength, unpacked);
      for (int i = 0; i < batchLength; i++) {
        Assert.assertEquals(forwardIndex[startDocId + i], unpacked[i]);
      }
    }

    // for each batch:
    // unaligned read on the first byte to unpack 3 integers
    // followed by 3 aligned reads at byte boundary to unpack 4 integers after each read -- 12 integers unpacked
    // followed by reading the next byte to unpack 2 integer from first 4 bits
    // 3 + 12 + 2  = 17 unpacked integers
    batchLength = 17;
    unpacked = new int[batchLength];
    startDocId = 1;
    bitSet.readInt(startDocId, batchLength, unpacked);
    for (int i = 0; i < batchLength; i++) {
      Assert.assertEquals(forwardIndex[startDocId + i], unpacked[i]);
    }

    // exercise all cases in PinotDataBitSetV2.Bit2Encoded
    // for each batch:
    // unaligned read on the first byte to unpack 3 integers (bits 2 to 7)
    // followed by 3 aligned reads at 4-byte boundary to unpack 16 integers after each read -- 48 integers unpacked
    // followed by 1 aligned read at byte boundary to unpack 4 integers -- 4 integers unpacked
    // followed by reading the next byte to unpack 3 integers from first 6 bits
    // 3 + 48 + 4 + 3 = 58 unpacked integers
    batchLength = 58;
    unpacked = new int[batchLength];
    startDocId = 1;
    bitSet.readInt(startDocId, batchLength, unpacked);
    for (int i = 0; i < batchLength; i++) {
      Assert.assertEquals(forwardIndex[startDocId + i], unpacked[i]);
    }

    // for each batch:
    // unaligned read on the first byte to unpack 2 integers (bits 4 to 7)
    // followed by 3 aligned reads at 4-byte boundary to unpack 16 integers after each read -- 48 integers unpacked
    // followed by 2 aligned reads at byte boundary to unpack 4 integers after each read -- 8 integers unpacked
    // 3 + 48 + 8 = 58 unpacked integers
    startDocId = 2;
    unpacked = new int[batchLength];
    bitSet.readInt(startDocId, batchLength, unpacked);
    for (int i = 0; i < batchLength; i++) {
      Assert.assertEquals(forwardIndex[startDocId + i], unpacked[i]);
    }

    // for each batch:
    // unaligned read on the first byte to unpack 1 integers (bits 6 to 7)
    // followed by 3 aligned reads at 4-byte boundary to unpack 16 integers after each read -- 48 integers unpacked
    // followed by 2 aligned reads at byte boundary to unpack 4 integers after each read -- 8 integers unpacked
    // followed by reading the next byte to unpack 1 integer from first 2 bits
    // 1 + 48 + 8 + 1 = 58 unpacked integers
    startDocId = 3;
    unpacked = new int[batchLength];
    bitSet.readInt(startDocId, batchLength, unpacked);
    for (int i = 0; i < batchLength; i++) {
      Assert.assertEquals(forwardIndex[startDocId + i], unpacked[i]);
    }

    bitSet.close();
  }

  @Test
  public void testBit4Encoded() throws Exception {
    int cardinality = 11;
    int rows = 10000;
    int[] forwardIndex = new int[rows];
    Random random = new Random();

    for (int i = 0; i < rows; i++) {
      forwardIndex[i] = random.nextInt(cardinality);
    }

    int numBitsPerValue = PinotDataBitSet.getNumBitsPerValue(cardinality - 1);
    int bitPackedBufferSize = (rows * numBitsPerValue + Byte.SIZE - 1) / Byte.SIZE;
    PinotDataBitSetV2 bitSet = getEmptyBitSet(bitPackedBufferSize, numBitsPerValue);

    Assert.assertEquals(4, numBitsPerValue);
    Assert.assertTrue(bitSet instanceof PinotDataBitSetV2.Bit4Encoded);

    for (int i = 0; i < rows; i++) {
      bitSet.writeInt(i, forwardIndex[i]);
    }

    for (int i = 0; i < rows; i++) {
      int unpacked = bitSet.readInt(i);
      Assert.assertEquals(forwardIndex[i], unpacked);
    }

    // for each batch:
    // 6 aligned reads at 4-byte boundary to unpack 8 integers after each read -- 48 integers unpacked
    // followed by reading the next byte to unpack 2 integers
    int batchLength = 50;
    int[] unpacked = new int[batchLength];
    int startDocId = 0;
    for (startDocId = 0; startDocId < rows; startDocId += batchLength) {
      bitSet.readInt(startDocId, batchLength, unpacked);
      for (int i = 0; i < batchLength; i++) {
        Assert.assertEquals(forwardIndex[startDocId + i], unpacked[i]);
      }
    }

    // unaligned read within a byte to unpack an integer from bits 4 to 7
    // followed by 2 aligned reads at 4-byte boundary to unpack 8 integers after each read -- unpacked 16 integers
    // followed by 1 aligned read at byte boundary to unpack 2 integers
    // followed by reading the next byte to unpack integer from first 4 bits
    // 1 + 16 + 2 + 1 = 20 unpacked integers
    startDocId = 1;
    batchLength = 20;
    unpacked = new int[batchLength];
    bitSet.readInt(startDocId, batchLength, unpacked);
    for (int i = 0; i < batchLength; i++) {
      Assert.assertEquals(forwardIndex[startDocId + i], unpacked[i]);
    }
    bitSet.close();
  }

  @Test
  public void testBit8Encoded() throws Exception {
    int cardinality = 190;
    int rows = 10000;
    int[] forwardIndex = new int[rows];
    Random random = new Random();

    for (int i = 0; i < rows; i++) {
      forwardIndex[i] = random.nextInt(cardinality);
    }

    int numBitsPerValue = PinotDataBitSet.getNumBitsPerValue(cardinality - 1);
    int bitPackedBufferSize = (rows * numBitsPerValue + Byte.SIZE - 1) / Byte.SIZE;
    PinotDataBitSetV2 bitSet = getEmptyBitSet(bitPackedBufferSize, numBitsPerValue);

    Assert.assertEquals(8, numBitsPerValue);
    Assert.assertTrue(bitSet instanceof PinotDataBitSetV2.Bit8Encoded);

    for (int i = 0; i < rows; i++) {
      bitSet.writeInt(i, forwardIndex[i]);
    }

    for (int i = 0; i < rows; i++) {
      int unpacked = bitSet.readInt(i);
      Assert.assertEquals(forwardIndex[i], unpacked);
    }

    // for each batch:
    // 12 aligned reads at 4-byte boundary to unpack 4 integers after each read -- 48 integers unpacked
    // followed by reading the next 2 bytes to unpack 2 integers
    int batchLength = 50;
    int[] unpacked = new int[batchLength];
    int startDocId = 0;
    for (startDocId = 0; startDocId < rows; startDocId += batchLength) {
      bitSet.readInt(startDocId, batchLength, unpacked);
      for (int i = 0; i < batchLength; i++) {
        Assert.assertEquals(forwardIndex[startDocId + i], unpacked[i]);
      }
    }

    bitSet.close();
  }

  @Test
  public void testBit16Encoded() throws Exception {
    int cardinality = 40000;
    int rows = 10000;
    int[] forwardIndex = new int[rows];
    Random random = new Random();

    for (int i = 0; i < rows; i++) {
      forwardIndex[i] = random.nextInt(cardinality);
    }

    int numBitsPerValue = PinotDataBitSet.getNumBitsPerValue(cardinality - 1);
    int bitPackedBufferSize = (rows * numBitsPerValue + Byte.SIZE - 1) / Byte.SIZE;
    PinotDataBitSetV2 bitSet = getEmptyBitSet(bitPackedBufferSize, numBitsPerValue);

    Assert.assertEquals(16, numBitsPerValue);
    Assert.assertTrue(bitSet instanceof PinotDataBitSetV2.Bit16Encoded);

    for (int i = 0; i < rows; i++) {
      bitSet.writeInt(i, forwardIndex[i]);
    }

    // test single read API for sequential consecutive
    for (int i = 0; i < rows; i++) {
      int unpacked = bitSet.readInt(i);
      Assert.assertEquals(forwardIndex[i], unpacked);
    }

    // test bulk API for sequential consecutive

    // for each batch:
    // 25 aligned reads at 4-byte boundary to unpack 2 integers after each read -- 50 integers unpacked
    int batchLength = 50;
    int[] unpacked = new int[batchLength];
    int startDocId = 0;
    for (startDocId = 0; startDocId < rows; startDocId += batchLength) {
      bitSet.readInt(startDocId, batchLength, unpacked);
      for (int i = 0; i < batchLength; i++) {
        Assert.assertEquals(forwardIndex[startDocId + i], unpacked[i]);
      }
    }

    // 25 aligned reads at 4-byte boundary to unpack 2 integers after each read -- 50 integers unpacked
    // followed by unpacking 1 integer from the next 2 bytes
    batchLength = 51;
    startDocId = 3;
    unpacked = new int[batchLength];
    bitSet.readInt(startDocId, batchLength, unpacked);
    for (int i = 0; i < batchLength; i++) {
      Assert.assertEquals(forwardIndex[startDocId + i], unpacked[i]);
    }

    // test bulk API for sequential but not necessarily consecutive
    batchLength = 50;
    int docId = 5;
    int[] docIds = new int[batchLength];
    random = new Random();
    for (int i = 0; i < batchLength; i++) {
      docId = docId + 1 + random.nextInt(5);
      docIds[i] = docId;
    }
    unpacked = new int[batchLength];
    bitSet.readInt(docIds, 0, batchLength, unpacked, 0);
    for (int i = 0; i < batchLength; i++) {
      Assert.assertEquals(forwardIndex[docIds[i]], unpacked[i]);
    }

    bitSet.close();
  }

  private PinotDataBitSetV2 getEmptyBitSet(int size, int numBitsPerValue) {
    PinotDataBuffer bitPackedBuffer = PinotDataBuffer.allocateDirect(size, ByteOrder.BIG_ENDIAN, null);
    for (int i = 0; i < size; i++) {
      bitPackedBuffer.readFrom(0, new byte[size]);
    }
    return PinotDataBitSetV2.createBitSet(bitPackedBuffer, numBitsPerValue);
  }
}
