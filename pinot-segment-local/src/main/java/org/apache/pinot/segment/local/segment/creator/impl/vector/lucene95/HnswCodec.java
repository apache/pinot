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
package org.apache.pinot.segment.local.segment.creator.impl.vector.lucene95;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.codecs.lucene90.Lucene90PointsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90PostingsFormat;
import org.apache.lucene.codecs.lucene95.Lucene95Codec;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;


/**
 * Extend the Lucene 9.5 index format
 * The major change here is to allow custom: @link{org.apache.lucene.codecs.KnnVectorsFormat}
 *
 * @see org.apache.lucene.codecs.lucene95 package documentation for file format details.
 */
public class HnswCodec extends FilterCodec {

  private final PostingsFormat _defaultPostingsFormat;
  private final PostingsFormat _postingsFormat =
      new PerFieldPostingsFormat() {
        @Override
        public PostingsFormat getPostingsFormatForField(String field) {
          return HnswCodec.this.getPostingsFormatForField(field);
        }
      };

  private final DocValuesFormat _defaultDVFormat;
  private final DocValuesFormat _docValuesFormat =
      new PerFieldDocValuesFormat() {
        @Override
        public DocValuesFormat getDocValuesFormatForField(String field) {
          return HnswCodec.this.getDocValuesFormatForField(field);
        }
      };

  private final KnnVectorsFormat _knnVectorsFormat =
      new PerFieldKnnVectorsFormat() {
        @Override
        public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
          return HnswCodec.this.getKnnVectorsFormatForField(field);
        }
      };

  private final KnnVectorsFormat _defaultKnnVectorsFormat;

  /**
   * Instantiates a new codec, specifying the stored fields compression mode to use.
   *
   * @param mode stored fields compression mode to use for newly flushed/merged segments.
   */
  public HnswCodec(Lucene95Codec.Mode mode, KnnVectorsFormat defaultKnnVectorsFormat) {
    super("Lucene95", new Lucene95Codec(mode));
    _defaultKnnVectorsFormat = defaultKnnVectorsFormat;
    _defaultPostingsFormat = new Lucene90PostingsFormat();
    _defaultDVFormat = new Lucene90DocValuesFormat();
  }

  @Override
  public final PostingsFormat postingsFormat() {
    return _postingsFormat;
  }

  @Override
  public final PointsFormat pointsFormat() {
    return new Lucene90PointsFormat();
  }

  @Override
  public final KnnVectorsFormat knnVectorsFormat() {
    return _knnVectorsFormat;
  }

  /**
   * Returns the postings format that should be used for writing new segments of <code>field</code>.
   *
   * <p>The default implementation always returns "Lucene90".
   *
   * <p><b>WARNING:</b> if you subclass, you are responsible for index backwards compatibility:
   * future version of Lucene are only guaranteed to be able to read the default implementation,
   */
  public PostingsFormat getPostingsFormatForField(String field) {
    return _defaultPostingsFormat;
  }

  /**
   * Returns the docvalues format that should be used for writing new segments of <code>field</code>
   * .
   *
   * <p>The default implementation always returns "Lucene90".
   *
   * <p><b>WARNING:</b> if you subclass, you are responsible for index backwards compatibility:
   * future version of Lucene are only guaranteed to be able to read the default implementation.
   */
  public DocValuesFormat getDocValuesFormatForField(String field) {
    return _defaultDVFormat;
  }

  /**
   * Returns the vectors format that should be used for writing new segments of <code>field</code>
   *
   * <p>The default implementation always returns "Lucene95".
   *
   * <p><b>WARNING:</b> if you subclass, you are responsible for index backwards compatibility:
   * future version of Lucene are only guaranteed to be able to read the default implementation.
   */
  public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
    return _defaultKnnVectorsFormat;
  }

  @Override
  public final DocValuesFormat docValuesFormat() {
    return _docValuesFormat;
  }
}
