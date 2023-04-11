package org.apache.pinot.segment.local.dedup;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.tuple.Pair;
import org.rocksdb.RocksDBException;

import java.util.List;

public interface LocalKeyValueStore {
    byte[] get(byte[] key);

    void delete(byte[] key);

    void put(byte[] key, byte[] value);

    void putBatch(List<Pair<byte[], byte[]>> keyValues);

    long getKeyCount();

    @VisibleForTesting
    void compact()
            throws RocksDBException;
}
