package com.linkedin.pinot.core.io.readerwriter.impl;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import com.linkedin.pinot.core.io.writer.impl.DirectMemoryManager;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ReaderWriterThreadSafetyTests {
  private static final int NUM_READERS = 4;
  private static final long RANDOM_SEED = System.currentTimeMillis();
  private static final Random RANDOM = new Random(RANDOM_SEED);
  private static final int MAX_NUM_ENTRIES = 5_000_000;

  private static final int MAX_MULTI_VALUES = 1000;
  private static final int AVG_MULTI_VALUES = 32;

  private final ExecutorService _executorService = Executors.newFixedThreadPool(NUM_READERS + 1);

  private volatile int _numEntries = 0;

  @BeforeClass
  public void setup() {
    // Printing to stdout so that we can reproduce a test with the seed if it fails.
    System.out.println("Random seed = " + RANDOM_SEED);
  }

  @Test
  public void testFixedByteSingleColumnSingleValueReaderWriter() throws Exception {
    testFixedByteSingleColumnSingleValueReaderWriter(FieldSpec.DataType.INT);
    testFixedByteSingleColumnSingleValueReaderWriter(FieldSpec.DataType.LONG);
    testFixedByteSingleColumnSingleValueReaderWriter(FieldSpec.DataType.FLOAT);
    testFixedByteSingleColumnSingleValueReaderWriter(FieldSpec.DataType.DOUBLE);
  }

  private void testFixedByteSingleColumnSingleValueReaderWriter(FieldSpec.DataType dataType) throws Exception {

    int initialChunkSize = RANDOM.nextInt(5000) + 5000;  // some number between 5k and 10k
    for (int chunkSize = initialChunkSize; chunkSize < MAX_NUM_ENTRIES; chunkSize *= 2) {
      final PinotDataBufferMemoryManager memoryManager = new DirectMemoryManager(ReaderWriterThreadSafetyTests.class.getName());
      _numEntries = 0;
      FixedByteSingleColumnSingleValueReaderWriter readerWriter =
          new FixedByteSingleColumnSingleValueReaderWriter(chunkSize, getColumnSize(dataType), memoryManager, "test");
      SVWriter writer = new SVWriter(readerWriter, dataType);
      Future<Void>[] readerFutures = new Future[NUM_READERS];
      for (int i = 0; i < NUM_READERS; i++) {
        SVReader reader = new SVReader(readerWriter, dataType);
        readerFutures[i] = _executorService.submit(reader);
      }
      Future<Void> writerFuture = _executorService.submit(writer);

      writerFuture.get();
      for (int i = 0; i < NUM_READERS; i++) {
        readerFutures[i].get();
      }
      readerWriter.close();
      memoryManager.close();
    }
  }

  private int getColumnSize(FieldSpec.DataType dataType) throws Exception {
    switch (dataType) {
      case INT:
        return V1Constants.Numbers.INTEGER_SIZE;
      case LONG:
        return V1Constants.Numbers.LONG_SIZE;
      case DOUBLE:
        return V1Constants.Numbers.DOUBLE_SIZE;
      case FLOAT:
        return V1Constants.Numbers.FLOAT_SIZE;
      default:
        throw new IllegalArgumentException("Unsupported type " + dataType.name());
    }
  }

  class SVWriter implements Callable<Void>{
    private final FixedByteSingleColumnSingleValueReaderWriter _readerWriter;
    private final FieldSpec.DataType _dataType;
    SVWriter(FixedByteSingleColumnSingleValueReaderWriter readerWriter, FieldSpec.DataType dataType) {
      _readerWriter = readerWriter;
      _dataType = dataType;
    }

    @Override
    public Void call() throws Exception {
      for (int entry = 0; entry < MAX_NUM_ENTRIES; entry++) {
        writeEntry(entry);
      }
      return null;
    }

    private void writeEntry(int entry) {
      switch (_dataType) {
        case INT:
          _readerWriter.setInt(entry, entry);
          break;
        case LONG:
          _readerWriter.setLong(entry, entry);
          break;
        case FLOAT:
          _readerWriter.setFloat(entry, entry);
          break;
        case DOUBLE:
          _readerWriter.setDouble(entry, entry);
          break;
        default:
          throw new IllegalArgumentException("Unsupported");
      }
      _numEntries++;
    }
  }

  class SVReader implements Callable<Void>{
    private final FixedByteSingleColumnSingleValueReaderWriter _readerWriter;
    private final FieldSpec.DataType _dataType;
    SVReader(FixedByteSingleColumnSingleValueReaderWriter readerWriter, FieldSpec.DataType dataType) {
      _readerWriter = readerWriter;
      _dataType = dataType;
    }

    @Override
    public Void call() throws Exception {
      while (_numEntries < MAX_NUM_ENTRIES) {
        if (_numEntries == 0) {
          continue;
        }
        // Pick a random number to read.
        int numEntries = _numEntries;
        readEntry(RANDOM.nextInt(numEntries));
        if (numEntries > 1 && _dataType.equals(FieldSpec.DataType.INT)) {
          // readValues() API is supported only for int.
          int startEntry = RANDOM.nextInt(numEntries / 2);
          int numValues = RANDOM.nextInt(numEntries - startEntry);
          readEntries(startEntry, numValues);
        }
      }
      return null;
    }

    private void readEntry(int entry) {
      switch (_dataType) {
        case INT:
          int intVal = _readerWriter.getInt(entry);
          Assert.assertEquals(intVal, entry, "failed at entry " + entry);
          break;
        case LONG:
          long longVal = _readerWriter.getLong(entry);
          Assert.assertEquals(longVal, (long)entry, "failed at entry " + entry);
          break;
        case FLOAT:
          float floatVal = _readerWriter.getFloat(entry);
          Assert.assertEquals(floatVal, (float)entry, "failed at entry " + entry);
          break;
        case DOUBLE:
          double doubleVal = _readerWriter.getDouble(entry);
          Assert.assertEquals(doubleVal, (double) entry, "failed at entry " + entry);
          break;
        default:
          throw new IllegalArgumentException("Unsupported");
      }
    }

    private void readEntries(int startEntry, int numValues) {
      int[] rows = new int[numValues];
      for (int i = 0; i < numValues; i++) {
        rows[i] = startEntry + i;
      }
      int[] values = new int[numValues];
      _readerWriter.readValues(rows, 0, numValues, values, 0);
    }
  }

  @Test
  public void testFixedByteSingleColumnMultiValueReaderWriter() throws Exception {
    int initialChunkSize = RANDOM.nextInt(5000) + 5000;  // some number between 5k and 10k
    // For now, hard-coding the data type and column size since the only use if the multi-value
    // reader-writer class is with an INT type.
    final FieldSpec.DataType dataType = FieldSpec.DataType.INT;
    final int columnSize = getColumnSize(dataType);
    for (int chunkSize = initialChunkSize; chunkSize < MAX_NUM_ENTRIES/2; chunkSize *= 2) {
      final PinotDataBufferMemoryManager memoryManager = new DirectMemoryManager(ReaderWriterThreadSafetyTests.class.getName());
      _numEntries = 0;
      FixedByteSingleColumnMultiValueReaderWriter readerWriter =
          new FixedByteSingleColumnMultiValueReaderWriter(MAX_MULTI_VALUES, AVG_MULTI_VALUES, chunkSize, columnSize,
              memoryManager, "test");

      MVWriter writer = new MVWriter(readerWriter);
      Future<Void>[] readerFutures = new Future[NUM_READERS];
      for (int i = 0; i < NUM_READERS; i++) {
        MVReader reader = new MVReader(readerWriter);
        readerFutures[i] = _executorService.submit(reader);
      }
      Future<Void> writerFuture = _executorService.submit(writer);

      writerFuture.get();
      for (int i = 0; i < NUM_READERS; i++) {
        readerFutures[i].get();
      }
      readerWriter.close();
      memoryManager.close();
    }
  }

  class MVWriter implements Callable<Void> {
    private final FixedByteSingleColumnMultiValueReaderWriter _readerWriter;
    private final int[] _values = new int[MAX_MULTI_VALUES+1];

    MVWriter(FixedByteSingleColumnMultiValueReaderWriter readerWriter) {
      _readerWriter = readerWriter;
      for (int i = 1; i < MAX_MULTI_VALUES; i++) {
        _values[i] = RANDOM.nextInt();
      }
    }

    @Override
    public Void call() throws Exception {
      for (int entry = 0; entry < MAX_NUM_ENTRIES; entry++) {
        int numValues = pickNumValues();
        int[] values = new int[numValues+1];
        for (int i = 1; i < numValues; i++) {
          values[i] = _values[i -1];
        }
        values[0] = numValues;
        _readerWriter.setIntArray(entry, values);
        _numEntries++;
      }
      return null;
    }

    int pickNumValues() {
      int percent = RANDOM.nextInt(100);
      int maxNumValues = AVG_MULTI_VALUES * 2;
      if (percent > 90) {
        maxNumValues = MAX_MULTI_VALUES;
      }
      return RANDOM.nextInt(maxNumValues);
    }
  }

  class MVReader implements Callable<Void> {

    private final FixedByteSingleColumnMultiValueReaderWriter _readerWriter;

    MVReader(FixedByteSingleColumnMultiValueReaderWriter readerWriter) {
      _readerWriter = readerWriter;
    }

    @Override
    public Void call() throws Exception {
      int[] values = new int[MAX_MULTI_VALUES];
      while (_numEntries < MAX_NUM_ENTRIES) {
        if (_numEntries == 0) {
          continue;
        }
        // Pick a random row to read.
        int numEntries = _numEntries;
        int rowToRead = RANDOM.nextInt(numEntries);
        int numValues = _readerWriter.getIntArray(rowToRead, values);
        Assert.assertEquals(numValues-1, values[0]);
      }
      return null;
    }
  }
}
