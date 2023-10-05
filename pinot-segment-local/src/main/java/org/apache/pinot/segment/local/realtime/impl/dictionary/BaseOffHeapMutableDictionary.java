/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.realtime.impl.dictionary;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.segment.spi.index.mutable.MutableDictionary;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Off-heap mutable dictionaries have the following elements:
 * - A forward map from dictionary ID to the actual value.
 * - A reverse map from the value to the dictionary ID.
 *
 * This base class provides the reverse map functionality. The reverse map is realized using a list of off-heap
 * IntBuffers directly allocated.
 *
 * An on-heap overflow hashmap holds items that have hash collisions, until the overflow hashmap reaches a threshold
 * size. At this point, we add a new IntBuffer, and transfer items from the overflow hashmap into the newly allocated
 * buffer, and also create a new overflow hashmap to handle future collisions.
 *
 * Overflow on-heap hashmap is set to contain a max number of values as provided in the constructor. If number is 0
 * then overflow on-heap hashmap is disabled.
 *
 * To start with, we only have the on-heap overflow buffer. The IntBuffers are allocated when overflow hashmap reaches
 * a threshold number of entries.
 *
 * A buffer has N rows (N being a prime number) and NUM_COLUMNS columns, as below.
 * - The actual value for NUM_COLUMNS is yet to be tuned.
 * - Each cell in the buffer can hold one integer.
 *
 *                | col 0 | col 1 | ..... | col M-1 |
 *        ==========================================|
 *        row 0   |       |       |       |         |
 *        ==========================================|
 *        row 1   |       |       |       |
 *        ==========================================|
 *          .
 *          .
 *          .
 *        ==========================================|
 *        row N-1 |       |       |       |         |
 *        ==========================================|
 *
 * To start with, all cells are initialized to have NULL_VALUE_INDEX (indicating empty cell)
 * Here is the pseudo-code for indexing an item or finding the dictionary ID of an item.
 *
 * index(item) {
 *   foreach (iBuf: iBufList) {
 *     hash value into a row for the buffer.
 *     foreach (cell: rowOfCells) {
 *       if (cell is not occupied) {
 *         set it to dictId
 *         return
 *       } else if (item.equals(get(dictId)) {
 *         // item already present in dictionary
 *         return
 *       }
 *     }
 *   }
 *   oveflow.put(item, dictId)
 *   if (overflow.size() > threshold) {
 *     newSize = lastBufSize * expansionMultiple
 *     newBuf = allocateDirect(newSize)
 *     add newBuf to iBufList
 *     newOverflow = new HashMap()
 *     foreach (entry: overflow) {
 *       hash entry.key() into a row for newBuf
 *       foreach (cell : rowOfCells) {
 *         if (cell empty) {
 *           set cell to entry.value();
 *         }
 *       }
 *       if (we did not enter value above) {
 *         newOverflow.put(entry.key(), entry.value());
 *       }
 *     }
 *   }
 * }
 *
 * indexOf(item) {
 *   foreach (iBuf: iBufList) {
 *     hash value into a row for the buffer;
 *     foreach (cell : rowOfCells) {
 *       if (cell is not occupied) {
 *         return NULL_VALUE_INDEX;
 *       }
 *       if (cell is occupied && item.equals(get(dictId))) {
 *         return dictId;
 *       }
 *     }
 *   }
 *   if (overflow.contains(item)) {
 *     return overflow.get(item);
 *   }
 *   return NULL_VALUE_INDEX;
 * }
 *
 * The list of buffers and the overflow hash are maintained in a class (ValueToDictId) that is referenced via an
 * AtomicReference. This ensures that readers always get either the new version of these objects or the old version,
 * but not some inconsistent versions of these.
 *
 * It should be noted that this class assumes that there is one writer and multiple readers of the dictionary. It is
 * NOT safe for a multiple writer scenario.
 *
 * TODO
 * - It may be useful to implement a way to stop adding new items when the the number of buffers reaches a certain
 *   threshold. In this case, we could close the realtime segment, and start a new one with bigger buffers.
 */
public abstract class BaseOffHeapMutableDictionary implements MutableDictionary {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseOffHeapMutableDictionary.class);

  // List of primes from http://compoasso.free.fr/primelistweb/page/prime/liste_online_en.php
  private static final int[] PRIME_NUMBERS = new int[]{
      13, 127, 547, 1009, 2003, 3001, 4003, 5003, 7001, 9001, 10007, 12007, 14009, 16001, 18013, 20011, 40009, 60013,
      80021, 100003, 125113, 150011, 175003, 200003, 225023, 250007, 275003, 300007, 350003, 400009, 450001, 500009,
      600011, 700001, 800011, 900001, 1000003
  };

  // expansionMultiple setting as we add new buffers. A setting of 1 sets the new buffer to be
  // the same size as the last one added. Setting of 2 allocates a buffer twice as big as the
  // previous one. It is assumed that these values are powers of 2. It is a good idea to restrict
  // these to 1 or 2, but not higher values. This array can be arbitrarily long. If the number of
  // buffers exceeds the number of elements in the array, we use the last value.
  private static final int[] EXPANSION_MULTIPLES = new int[]{1, 1, 2, 2, 2};

  // Number of columns in each row of an IntBuffer.
  private static final int NUM_COLUMNS = 3;

  // Whether to start with 0 off-heap storage. Items are added to the overflow map first, until it reaches
  // a threshold, and then the off-heap structures are allocated.
  @SuppressWarnings("FieldCanBeLocal")
  private static boolean _heapFirst = true;

  private final int _maxItemsInOverflowHash;

  // Number of entries in the dictionary. Max dictId is _numEntries-1.
  private volatile int _numEntries;

  private final int _initialRowCount;
  private final PinotDataBufferMemoryManager _memoryManager;
  private final String _allocationContext;

  /**
   * A class to hold all the objects needed for the reverse mapping.
   */
  private static class ValueToDictId {
    // Each iBuf layout is as described in comments above.
    private final List<IntBuffer> _iBufList;
    // The map should be a concurrent one.
    private final Map<Object, Integer> _overflowMap;

    private ValueToDictId(List<IntBuffer> iBufList, Map<Object, Integer> overflowMap) {
      _iBufList = iBufList;
      _overflowMap = overflowMap;
    }

    private List<IntBuffer> getIBufList() {
      return _iBufList;
    }

    private Map<Object, Integer> getOverflowMap() {
      return _overflowMap;
    }
  }

  private volatile ValueToDictId _valueToDict;

  protected BaseOffHeapMutableDictionary(int estimatedCardinality, int maxOverflowHashSize,
      PinotDataBufferMemoryManager memoryManager, String allocationContext) {
    _memoryManager = memoryManager;
    _allocationContext = allocationContext;
    _initialRowCount = nearestPrime(estimatedCardinality);
    _maxItemsInOverflowHash = maxOverflowHashSize;
    init();
  }

  protected void init() {
    _numEntries = 0;
    _valueToDict = new ValueToDictId(new ArrayList<>(), new ConcurrentHashMap<>());
    if (!_heapFirst || (_maxItemsInOverflowHash == 0)) {
      expand(_initialRowCount, 1);
    }
  }

  @Override
  public int length() {
    return _numEntries;
  }

  @Override
  public void close()
      throws IOException {
    doClose();
  }

  private int nearestPrime(int size) {
    for (int primeNumber : PRIME_NUMBERS) {
      if (primeNumber >= size) {
        return primeNumber;
      }
    }
    return PRIME_NUMBERS[PRIME_NUMBERS.length - 1];
  }

  private long computeBBsize(long numRows) {
    return numRows * NUM_COLUMNS * Integer.BYTES;
  }

  // Assume prevNumRows is a valid value, and return it if the new one overflows.
  private int validatedNumRows(int prevNumRows, int multiple) {
    long newNumRows = (long) prevNumRows * multiple;

    if (newNumRows > Integer.MAX_VALUE) {
      return prevNumRows;
    }

    if (computeBBsize(newNumRows) > Integer.MAX_VALUE) {
      return prevNumRows;
    }
    return (int) newNumRows;
  }

  private IntBuffer expand(int prevNumRows, int multiple) {
    final int newNumRows = validatedNumRows(prevNumRows, multiple);
    final int bbSize = (int) computeBBsize(newNumRows);

    final ValueToDictId valueToDictId = _valueToDict;
    List<IntBuffer> oldList = valueToDictId.getIBufList();
    List<IntBuffer> newList = new ArrayList<>(oldList.size() + 1);
    for (IntBuffer iBuf : oldList) {
      newList.add(iBuf);
    }
    LOGGER.info("Allocating {} bytes for: {}", bbSize, _allocationContext);
    // NOTE: PinotDataBuffer is tracked in the PinotDataBufferMemoryManager. No need to track it inside the class.
    PinotDataBuffer buffer = _memoryManager.allocate(bbSize, _allocationContext);
    IntBuffer iBuf = buffer.toDirectByteBuffer(0L, bbSize).asIntBuffer();
    for (int i = 0; i < iBuf.capacity(); i++) {
      iBuf.put(i, NULL_VALUE_INDEX);
    }
    newList.add(iBuf);
    Map<Object, Integer> newOverflowMap = new ConcurrentHashMap<>(HashUtil.getHashMapCapacity(_maxItemsInOverflowHash));
    if (_maxItemsInOverflowHash > 0) {
      Map<Object, Integer> oldOverflowMap = valueToDictId.getOverflowMap();
      for (Map.Entry<Object, Integer> entry : oldOverflowMap.entrySet()) {
        final int hashVal = entry.getKey().hashCode() & Integer.MAX_VALUE;
        final int offsetInBuf = (hashVal % newNumRows) * NUM_COLUMNS;
        boolean done = false;
        for (int i = offsetInBuf; i < offsetInBuf + NUM_COLUMNS; i++) {
          if (iBuf.get(i) == NULL_VALUE_INDEX) {
            iBuf.put(i, entry.getValue());
            done = true;
            break;
          }
        }
        if (!done) {
          newOverflowMap.put(entry.getKey(), entry.getValue());
        }
      }
    }
    _valueToDict = new ValueToDictId(newList, newOverflowMap);
    // We should not clear oldOverflowMap or oldList here, as readers may still be accessing those.
    // We let GC take care of those elements.
    return iBuf;
  }

  private IntBuffer expand() {
    final ValueToDictId valueToDictId = _valueToDict;
    List<IntBuffer> iBufList = valueToDictId.getIBufList();
    final int numBuffers = iBufList.size();
    if (numBuffers == 0) {
      return expand(_initialRowCount, 1);
    }
    final int lastCapacity = iBufList.get(numBuffers - 1).capacity();
    int expansionMultiple = EXPANSION_MULTIPLES[EXPANSION_MULTIPLES.length - 1];
    if (numBuffers < EXPANSION_MULTIPLES.length) {
      expansionMultiple = EXPANSION_MULTIPLES[numBuffers];
    }
    int prevNumRows = lastCapacity / NUM_COLUMNS;
    return expand(prevNumRows, expansionMultiple);
  }

  protected int nearestPowerOf2(int num) {
    if ((num & (num - 1)) == 0) {
      return num;
    }
    int power = Integer.SIZE - Integer.numberOfLeadingZeros(num);
    Preconditions.checkState(power < Integer.SIZE - 1);
    return 1 << power;
  }

  /**
   * Given a raw value, get the dictionary ID from the reverse map.
   *
   * Since the dictionary IDs are stored in a hash map, multiple dictionary
   * IDs may match to the same raw value. Use the methods provided by sub-class
   * to compare the raw values with those in the forward map.
   *
   * @param value value of object for which we need to get dictionary ID
   * @param serializedValue serialized form of the
   * @return dictionary ID if found, NULL_VALUE_INDEX otherwise.
   */
  protected int getDictId(Object value, byte[] serializedValue) {
    final int hashVal = value.hashCode() & Integer.MAX_VALUE;
    final ValueToDictId valueToDictId = _valueToDict;
    final List<IntBuffer> iBufList = valueToDictId.getIBufList();
    for (IntBuffer iBuf : iBufList) {
      final int modulo = iBuf.capacity() / NUM_COLUMNS;
      final int offsetInBuf = (hashVal % modulo) * NUM_COLUMNS;
      for (int i = offsetInBuf; i < offsetInBuf + NUM_COLUMNS; i++) {
        int dictId = iBuf.get(i);
        if (dictId != NULL_VALUE_INDEX) {
          if (equalsValueAt(dictId, value, serializedValue)) {
            return dictId;
          }
        }
      }
    }
    if (_maxItemsInOverflowHash == 0) {
      return NULL_VALUE_INDEX;
    }
    Integer dictId = valueToDictId.getOverflowMap().get(value);
    if (dictId == null) {
      return NULL_VALUE_INDEX;
    }
    return dictId;
  }

  /**
   * Index a value into the forward map (dictionary ID to value) and the reverse map
   * (value to dictionary). Take care to set the reverse map last so as to make it
   * work correctly for single writer multiple reader threads. Insertion and comparison
   * methods for the forward map are provided by sub-classes.
   *
   * @param value value to be inserted into the dictionary
   * @param serializedValue serialized representation of the value, may be null.
   */
  protected int indexValue(Object value, byte[] serializedValue) {
    final int hashVal = value.hashCode() & Integer.MAX_VALUE;
    int newValueDictId = _numEntries;
    ValueToDictId valueToDictId = _valueToDict;

    for (IntBuffer iBuf : valueToDictId.getIBufList()) {
      final int modulo = iBuf.capacity() / NUM_COLUMNS;
      final int offsetInBuf = (hashVal % modulo) * NUM_COLUMNS;
      for (int i = offsetInBuf; i < offsetInBuf + NUM_COLUMNS; i++) {
        final int dictId = iBuf.get(i);
        if (dictId == NULL_VALUE_INDEX) {
          setValue(newValueDictId, value, serializedValue);
          iBuf.put(i, newValueDictId);
          _numEntries = newValueDictId + 1;
          return newValueDictId;
        } else if (equalsValueAt(dictId, value, serializedValue)) {
          return dictId;
        }
      }
    }
    // We know that we had a hash collision beyond the number of columns in the buffer.
    Map<Object, Integer> overflowMap = valueToDictId.getOverflowMap();
    if (_maxItemsInOverflowHash > 0) {
      Integer dictId = overflowMap.get(value);
      if (dictId != null) {
        return dictId;
      }
    }

    setValue(newValueDictId, value, serializedValue);

    if (_maxItemsInOverflowHash > 0) {
      if (overflowMap.size() < _maxItemsInOverflowHash) {
        overflowMap.put(value, newValueDictId);
        _numEntries = newValueDictId + 1;
        return newValueDictId;
      }
    }
    // Need a new buffer
    IntBuffer buf = expand();
    final int modulo = buf.capacity() / NUM_COLUMNS;
    final int offsetInBuf = (hashVal % modulo) * NUM_COLUMNS;
    for (int i = offsetInBuf; i < offsetInBuf + NUM_COLUMNS; i++) {
      if (buf.get(i) == NULL_VALUE_INDEX) {
        buf.put(i, newValueDictId);
        _numEntries = newValueDictId + 1;
        return newValueDictId;
      }
    }
    overflowMap = _valueToDict.getOverflowMap();
    overflowMap.put(value, newValueDictId);
    _numEntries = newValueDictId + 1;
    return newValueDictId;
  }

  protected long getOffHeapMemUsed() {
    ValueToDictId valueToDictId = _valueToDict;
    long size = 0;
    for (IntBuffer iBuf : valueToDictId._iBufList) {
      size = size + iBuf.capacity() * Integer.BYTES;
    }
    return size;
  }

  public int getNumberOfHeapBuffersUsed() {
    ValueToDictId valueToDictId = _valueToDict;
    return valueToDictId._iBufList.size();
  }

  public int getNumberOfOveflowValues() {
    ValueToDictId valueToDictId = _valueToDict;
    return valueToDictId._overflowMap.size();
  }

  protected abstract void setValue(int dictId, Object value, byte[] serializedValue);

  protected abstract boolean equalsValueAt(int dictId, Object value, byte[] serializedValue);

  public abstract int getAvgValueSize();

  public abstract long getTotalOffHeapMemUsed();

  protected abstract void doClose()
      throws IOException;
}
