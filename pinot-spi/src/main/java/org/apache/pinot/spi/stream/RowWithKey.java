package org.apache.pinot.spi.stream;

import java.util.Arrays;
import javax.annotation.Nullable;


/**
 * Helper class so the key and payload can be easily tied together instead of using pair
 */
public class RowWithKey {
  private final byte[] _key;
  private final byte[] _payload;

  public RowWithKey(@Nullable byte[] key, byte[] payload) {
    _key = key;
    _payload = payload;
  }

  public byte[] getKey() {
    return _key;
  }

  public byte[] getPayload() {
    return _payload;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RowWithKey that = (RowWithKey) o;
    return Arrays.equals(_key, that._key) && Arrays.equals(_payload, that._payload);
  }

  @Override
  public int hashCode() {
    int result = Arrays.hashCode(_key);
    result = 31 * result + Arrays.hashCode(_payload);
    return result;
  }
}
