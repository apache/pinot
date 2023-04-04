package org.apache.pinot.segment.local.dedup;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

public class LocalKeyValueStore {
  @VisibleForTesting
  public static final String DEDUP_DATA_DIR = "/tmp/dedup-data";
  @VisibleForTesting
  static final RocksDB ROCKS_DB = initRocksDB();

  @VisibleForTesting
  final ColumnFamilyHandle _columnFamilyHandle;

  private static RocksDB initRocksDB() {
      RocksDB.loadLibrary();
      final Options options = new Options();
      options.setCreateIfMissing(true);
      File dbDir = new File(DEDUP_DATA_DIR);
      try {
        return RocksDB.open(options, dbDir.getAbsolutePath());
      } catch (RocksDBException ex) {
        throw new RuntimeException(ex);
      }
  }

  public LocalKeyValueStore(byte[] id) {
    try {
      _columnFamilyHandle = ROCKS_DB.createColumnFamily(new ColumnFamilyDescriptor(id));
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  byte[] get(byte[] keyBytes) {
    try {
      return ROCKS_DB.get(_columnFamilyHandle, keyBytes);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  void delete(byte[] keyBytes) {
    try {
      ROCKS_DB.delete(_columnFamilyHandle, keyBytes);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  void put(byte[] keyBytes, byte[] valueBytes) {
    try {
      ROCKS_DB.put(_columnFamilyHandle, keyBytes, valueBytes);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  void putBatch(List<Pair<byte[], byte[]>> keyValue) {
    WriteBatch writeBatch = new WriteBatch();
    try {
      for (Pair<byte[], byte[]> pair : keyValue) {
        writeBatch.put(_columnFamilyHandle, pair.getKey(), pair.getValue());
      }
      LocalKeyValueStore.ROCKS_DB.write(new WriteOptions(), writeBatch);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  long getKeyCount() {
    try {
      return ROCKS_DB.getLongProperty(_columnFamilyHandle, "rocksdb.estimate-num-keys");
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  void compact()
      throws RocksDBException {
    ROCKS_DB.compactRange(_columnFamilyHandle);
  }
}
