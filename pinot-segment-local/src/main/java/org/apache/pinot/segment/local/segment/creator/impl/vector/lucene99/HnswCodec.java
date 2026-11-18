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
package org.apache.pinot.segment.local.segment.creator.impl.vector.lucene99;

import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;


/// Uses Lucene's standard codec with a configurable HNSW vector format.
/// Legacy codec names are resolved by Lucene's backward codecs when reading.
public class HnswCodec extends FilterCodec {
  private final KnnVectorsFormat _defaultKnnVectorsFormat;
  private final KnnVectorsFormat _knnVectorsFormat = new PerFieldKnnVectorsFormat() {
    @Override
    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
      return HnswCodec.this.getKnnVectorsFormatForField(field);
    }
  };

  /// Instantiates a codec with the given stored-fields compression mode and vector format.
  public HnswCodec(Lucene104Codec.Mode mode, KnnVectorsFormat defaultKnnVectorsFormat) {
    this(new Lucene104Codec(mode), defaultKnnVectorsFormat);
  }

  private HnswCodec(Lucene104Codec codec, KnnVectorsFormat defaultKnnVectorsFormat) {
    // Persist the delegate's registered codec name so readers can open the index.
    super(codec.getName(), codec);
    _defaultKnnVectorsFormat = defaultKnnVectorsFormat;
  }

  @Override
  public final KnnVectorsFormat knnVectorsFormat() {
    return _knnVectorsFormat;
  }

  /// Returns the vectors format for writing new segments of `field`.
  public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
    return _defaultKnnVectorsFormat;
  }
}
