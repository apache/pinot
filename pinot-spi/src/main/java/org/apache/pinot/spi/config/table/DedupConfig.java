package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.pinot.spi.config.BaseJsonConfig;

public class DedupConfig extends BaseJsonConfig {
  private final boolean _dedupEnabled;
  private final HashFunction _hashFunction;

  @JsonCreator
  public DedupConfig(@JsonProperty(value = "dedupEnabled", required = true) final boolean dedupEnabled,
      @JsonProperty(value = "hashFunction") final HashFunction hashFunction
  ) {
    this._dedupEnabled = dedupEnabled;
    this._hashFunction = hashFunction;
  }

  public HashFunction getHashFunction() {
    return _hashFunction;
  }

  public boolean isDedupEnabled() {
    return _dedupEnabled;
  }
}
