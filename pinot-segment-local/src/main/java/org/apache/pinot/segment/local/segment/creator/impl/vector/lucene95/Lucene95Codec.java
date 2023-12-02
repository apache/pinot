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

import java.util.Objects;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90CompoundFormat;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.codecs.lucene90.Lucene90LiveDocsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90NormsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90PointsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90PostingsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90SegmentInfoFormat;
import org.apache.lucene.codecs.lucene90.Lucene90StoredFieldsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90TermVectorsFormat;
import org.apache.lucene.codecs.lucene94.Lucene94FieldInfosFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;


/**
 * Implements the Lucene 9.5 index format
 *
 * <p>If you want to reuse functionality of this codec in another codec, extend {@link FilterCodec}.
 *
 * @lucene.experimental
 * @see org.apache.lucene.codecs.lucene95 package documentation for file format details.
 */
public class Lucene95Codec extends Codec {

  /**
   * Configuration option for the codec.
   */
  public enum Mode {
    /**
     * Trade compression ratio for retrieval speed.
     */
    BEST_SPEED(Lucene90StoredFieldsFormat.Mode.BEST_SPEED),
    /**
     * Trade retrieval speed for compression ratio.
     */
    BEST_COMPRESSION(Lucene90StoredFieldsFormat.Mode.BEST_COMPRESSION);

    private final Lucene90StoredFieldsFormat.Mode _storedMode;

    private Mode(Lucene90StoredFieldsFormat.Mode storedMode) {
      _storedMode = Objects.requireNonNull(storedMode);
    }
  }

  private final TermVectorsFormat _vectorsFormat = new Lucene90TermVectorsFormat();
  private final FieldInfosFormat _fieldInfosFormat = new Lucene94FieldInfosFormat();
  private final SegmentInfoFormat _segmentInfosFormat = new Lucene90SegmentInfoFormat();
  private final LiveDocsFormat _liveDocsFormat = new Lucene90LiveDocsFormat();
  private final CompoundFormat _compoundFormat = new Lucene90CompoundFormat();
  private final NormsFormat _normsFormat = new Lucene90NormsFormat();

  private final PostingsFormat _defaultPostingsFormat;
  private final PostingsFormat _postingsFormat =
      new PerFieldPostingsFormat() {
        @Override
        public PostingsFormat getPostingsFormatForField(String field) {
          return Lucene95Codec.this.getPostingsFormatForField(field);
        }
      };

  private final DocValuesFormat _defaultDVFormat;
  private final DocValuesFormat _docValuesFormat =
      new PerFieldDocValuesFormat() {
        @Override
        public DocValuesFormat getDocValuesFormatForField(String field) {
          return Lucene95Codec.this.getDocValuesFormatForField(field);
        }
      };

  private final KnnVectorsFormat _defaultKnnVectorsFormat;
  private final KnnVectorsFormat _knnVectorsFormat =
      new PerFieldKnnVectorsFormat() {
        @Override
        public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
          return Lucene95Codec.this.getKnnVectorsFormatForField(field);
        }
      };

  private final StoredFieldsFormat _storedFieldsFormat;

  /**
   * Instantiates a new codec.
   */
  public Lucene95Codec() {
    this(Mode.BEST_SPEED);
  }

  /**
   * Instantiates a new codec, specifying the stored fields compression mode to use.
   *
   * @param mode stored fields compression mode to use for newly flushed/merged segments.
   */
  public Lucene95Codec(Mode mode) {
    this(mode, new Lucene95HnswVectorsFormat());
  }

  /**
   * Instantiates a new codec, specifying the stored fields compression mode to use.
   *
   * @param mode stored fields compression mode to use for newly flushed/merged segments.
   */
  public Lucene95Codec(Mode mode, KnnVectorsFormat defaultKnnVectorsFormat) {
    super("Lucene95");
    _storedFieldsFormat =
        new Lucene90StoredFieldsFormat(Objects.requireNonNull(mode)._storedMode);
    _defaultPostingsFormat = new Lucene90PostingsFormat();
    _defaultDVFormat = new Lucene90DocValuesFormat();
    _defaultKnnVectorsFormat = defaultKnnVectorsFormat;
  }

  @Override
  public final StoredFieldsFormat storedFieldsFormat() {
    return _storedFieldsFormat;
  }

  @Override
  public final TermVectorsFormat termVectorsFormat() {
    return _vectorsFormat;
  }

  @Override
  public final PostingsFormat postingsFormat() {
    return _postingsFormat;
  }

  @Override
  public final FieldInfosFormat fieldInfosFormat() {
    return _fieldInfosFormat;
  }

  @Override
  public final SegmentInfoFormat segmentInfoFormat() {
    return _segmentInfosFormat;
  }

  @Override
  public final LiveDocsFormat liveDocsFormat() {
    return _liveDocsFormat;
  }

  @Override
  public final CompoundFormat compoundFormat() {
    return _compoundFormat;
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

  @Override
  public final NormsFormat normsFormat() {
    return _normsFormat;
  }
}
