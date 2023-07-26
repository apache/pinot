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

package org.apache.pinot.segment.spi.index;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.FSTType;
import org.apache.pinot.spi.config.table.IndexConfig;


public class TextIndexConfig extends IndexConfig {
  public static final TextIndexConfig DISABLED = new TextIndexConfig(true, null, null, false, false,
      Collections.emptyList(), Collections.emptyList());
  private final FSTType _fstType;
  @Nullable
  private final Object _rawValueForTextIndex;
  private final boolean _enableQueryCache;
  private final boolean _useANDForMultiTermQueries;
  private final List<String> _stopWordsInclude;
  private final List<String> _stopWordsExclude;

  @JsonCreator
  public TextIndexConfig(
      @JsonProperty("disabled") Boolean disabled,
      @JsonProperty("fst") FSTType fstType,
      @JsonProperty("rawValue") @Nullable Object rawValueForTextIndex,
      @JsonProperty("queryCache") boolean enableQueryCache,
      @JsonProperty("useANDForMultiTermQueries") boolean useANDForMultiTermQueries,
      @JsonProperty("stopWordsInclude") List<String> stopWordsInclude,
      @JsonProperty("stopWordsExclude") List<String> stopWordsExclude) {
    super(disabled);
    _fstType = fstType;
    _rawValueForTextIndex = rawValueForTextIndex;
    _enableQueryCache = enableQueryCache;
    _useANDForMultiTermQueries = useANDForMultiTermQueries;
    _stopWordsInclude = stopWordsInclude;
    _stopWordsExclude = stopWordsExclude;
  }

  public FSTType getFstType() {
    return _fstType;
  }

  @Nullable
  public Object getRawValueForTextIndex() {
    return _rawValueForTextIndex;
  }

  /**
   * Whether Lucene query result cache should be enabled.
   *
   * While it helps a lot with performance for repeated queries, on the downside it causes heap issues.
   */
  public boolean isEnableQueryCache() {
    return _enableQueryCache;
  }

  public boolean isUseANDForMultiTermQueries() {
    return _useANDForMultiTermQueries;
  }

  public List<String> getStopWordsInclude() {
    return _stopWordsInclude;
  }

  public List<String> getStopWordsExclude() {
    return _stopWordsExclude;
  }

  public static abstract class AbstractBuilder {
    @Nullable
    protected FSTType _fstType;
    @Nullable
    protected Object _rawValueForTextIndex;
    protected boolean _enableQueryCache = false;
    protected boolean _useANDForMultiTermQueries = true;
    protected List<String> _stopWordsInclude = new ArrayList<>();
    protected List<String> _stopWordsExclude = new ArrayList<>();

    public AbstractBuilder(@Nullable FSTType fstType) {
      _fstType = fstType;
    }

    public AbstractBuilder(TextIndexConfig other) {
      _fstType = other._fstType;
      _enableQueryCache = other._enableQueryCache;
      _useANDForMultiTermQueries = other._useANDForMultiTermQueries;
      _stopWordsInclude = new ArrayList<>(other._stopWordsInclude);
      _stopWordsExclude = new ArrayList<>(other._stopWordsExclude);
    }

    public TextIndexConfig build() {
      return new TextIndexConfig(false, _fstType, _rawValueForTextIndex, _enableQueryCache, _useANDForMultiTermQueries,
          _stopWordsInclude, _stopWordsExclude);
    }

    public abstract AbstractBuilder withProperties(@Nullable Map<String, String> textIndexProperties);

    public AbstractBuilder withRawValueForTextIndex(@Nullable Object rawValueForTextIndex) {
      _rawValueForTextIndex = rawValueForTextIndex;
      return this;
    }

    public AbstractBuilder withStopWordsInclude(List<String> stopWordsInclude) {
      _stopWordsInclude = stopWordsInclude;
      return this;
    }

    public AbstractBuilder withStopWordsExclude(List<String> stopWordsExclude) {
      _stopWordsExclude = stopWordsExclude;
      return this;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    TextIndexConfig that = (TextIndexConfig) o;
    return _enableQueryCache == that._enableQueryCache && _useANDForMultiTermQueries == that._useANDForMultiTermQueries
        && _fstType == that._fstType && Objects.equals(_rawValueForTextIndex, that._rawValueForTextIndex)
        && Objects.equals(_stopWordsInclude, that._stopWordsInclude) && Objects.equals(_stopWordsExclude,
        that._stopWordsExclude);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _fstType, _rawValueForTextIndex, _enableQueryCache,
        _useANDForMultiTermQueries, _stopWordsInclude, _stopWordsExclude);
  }
}
