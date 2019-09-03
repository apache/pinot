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
package org.apache.pinot.core.minion;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import javax.annotation.Nonnull;
import org.apache.pinot.common.data.DateTimeFieldSpec;
import org.apache.pinot.common.data.DateTimeFormatSpec;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.data.StarTreeIndexSpec;
import org.apache.pinot.common.data.TimeFieldSpec;
import org.apache.pinot.common.data.TimeGranularitySpec;
import org.apache.pinot.common.segment.StarTreeMetadata;
import org.apache.pinot.core.data.GenericRow;
import org.apache.pinot.core.data.readers.FileFormat;
import org.apache.pinot.core.data.readers.PinotSegmentRecordReader;
import org.apache.pinot.core.data.readers.RecordReader;
import org.apache.pinot.core.data.recordtransformer.CompositeTransformer;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.segment.creator.RecordReaderSegmentCreationDataSource;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.core.segment.index.SegmentMetadataImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>BackfillDateTimeColumn</code> class takes a segment, a timeSpec from the segment, and a
 * dateTimeSpec.
 * It creates a new segment with a new column corresponding to the dateTimeSpec configs, using the values from the timeSpec
 * <ul>
 *  <li>If a column corresponding to the dateTimeSpec already exists, it is overwritten</li>
 *  <li>If not, a new date time column is created</li>
 *  <li>If the segment contains star tree, it is recreated, putting date time column at the end</li>
 * </ul>
 * <p>
 */
public class BackfillDateTimeColumn {
  private static final Logger LOGGER = LoggerFactory.getLogger(BackfillDateTimeColumn.class);

  private final String _rawTableName;
  private final File _originalIndexDir;
  private final File _backfilledIndexDir;
  private final TimeFieldSpec _srcTimeFieldSpec;
  private final DateTimeFieldSpec _destDateTimeFieldSpec;

  public BackfillDateTimeColumn(@Nonnull String rawTableName, @Nonnull File originalIndexDir, @Nonnull File backfilledIndexDir,
      @Nonnull TimeFieldSpec srcTimeSpec, @Nonnull DateTimeFieldSpec destDateTimeSpec)
      throws Exception {
    _rawTableName = rawTableName;
    _originalIndexDir = originalIndexDir;
    _backfilledIndexDir = backfilledIndexDir;
    Preconditions.checkArgument(!_originalIndexDir.getAbsolutePath().equals(_backfilledIndexDir.getAbsolutePath()),
        "Original index dir and backfill index dir should not be the same");
    _srcTimeFieldSpec = srcTimeSpec;
    _destDateTimeFieldSpec = destDateTimeSpec;
  }

  public boolean backfill()
      throws Exception {
    SegmentMetadataImpl originalSegmentMetadata = new SegmentMetadataImpl(_originalIndexDir);
    String segmentName = originalSegmentMetadata.getName();
    LOGGER.info("Start backfilling segment: {} in table: {}", segmentName, _rawTableName);

    PinotSegmentRecordReader segmentRecordReader = new PinotSegmentRecordReader(_originalIndexDir);
    BackfillDateTimeRecordReader wrapperReader =
        new BackfillDateTimeRecordReader(segmentRecordReader, _srcTimeFieldSpec, _destDateTimeFieldSpec);
    LOGGER.info("Segment dir: {} Output Dir: {}", _originalIndexDir.getAbsolutePath(),
        _backfilledIndexDir.getAbsolutePath());

    LOGGER.info("Creating segment generator config for {}", segmentName);
    SegmentGeneratorConfig config = new SegmentGeneratorConfig();
    config.setFormat(FileFormat.PINOT);
    config.setOutDir(_backfilledIndexDir.getAbsolutePath());
    config.setOverwrite(true);
    config.setTableName(_rawTableName);
    config.setSegmentName(segmentName);
    config.setSchema(wrapperReader.getSchema());

    StarTreeMetadata starTreeMetadata = originalSegmentMetadata.getStarTreeMetadata();
    if (starTreeMetadata != null) {
      config.enableStarTreeIndex(StarTreeIndexSpec.fromStarTreeMetadata(starTreeMetadata));
    }

    LOGGER.info("Creating segment for {} with config {}", segmentName, config.toString());
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new RecordReaderSegmentCreationDataSource(wrapperReader),
        CompositeTransformer.getPassThroughTransformer());
    driver.build();

    return true;
  }

  public BackfillDateTimeRecordReader getBackfillDateTimeRecordReader(RecordReader baseRecordReader) {
    return new BackfillDateTimeRecordReader(baseRecordReader, _srcTimeFieldSpec, _destDateTimeFieldSpec);
  }

  /**
   * This record reader is a wrapper over another record reader.
   * It simply reads the records from the base record reader, and adds a new field according to the
   * dateTimeFieldSpec
   */
  public class BackfillDateTimeRecordReader implements RecordReader {
    private final RecordReader _baseRecordReader;
    private final TimeFieldSpec _timeFieldSpec;
    private final DateTimeFieldSpec _dateTimeFieldSpec;
    private final Schema _schema;

    public BackfillDateTimeRecordReader(RecordReader baseRecordReader, TimeFieldSpec timeFieldSpec,
        DateTimeFieldSpec dateTimeFieldSpec) {
      _baseRecordReader = baseRecordReader;
      _timeFieldSpec = timeFieldSpec;
      _dateTimeFieldSpec = dateTimeFieldSpec;
      _schema = baseRecordReader.getSchema();

      // Add/replace the date time field spec to the schema
      _schema.removeField(_dateTimeFieldSpec.getName());
      _schema.addField(_dateTimeFieldSpec);
    }

    @Override
    public void init(SegmentGeneratorConfig segmentGeneratorConfig) {

    }

    @Override
    public boolean hasNext() {
      return _baseRecordReader.hasNext();
    }

    @Override
    public GenericRow next()
        throws IOException {
      return next(new GenericRow());
    }

    /**
     * Reads the next row from the baseRecordReader, and adds a dateTimeFieldSPec column to it
     * {@inheritDoc}
     * @see org.apache.pinot.core.data.readers.RecordReader#next(org.apache.pinot.core.data.GenericRow)
     */
    @Override
    public GenericRow next(GenericRow reuse)
        throws IOException {
      reuse = _baseRecordReader.next(reuse);
      Long timeColumnValue = (Long) reuse.getValue(_timeFieldSpec.getName());
      Object dateTimeColumnValue = convertTimeFieldToDateTimeFieldSpec(timeColumnValue);
      reuse.putField(_dateTimeFieldSpec.getName(), dateTimeColumnValue);
      return reuse;
    }

    /**
     * Converts the time column value from timeFieldSpec to dateTimeFieldSpec
     * @param timeColumnValue - time column value from timeFieldSpec
     * @return
     */
    private Object convertTimeFieldToDateTimeFieldSpec(Object timeColumnValue) {
      TimeGranularitySpec timeGranularitySpec = _timeFieldSpec.getOutgoingGranularitySpec();

      DateTimeFormatSpec formatFromTimeSpec =
          new DateTimeFormatSpec(timeGranularitySpec.getTimeUnitSize(), timeGranularitySpec.getTimeType().toString(),
              timeGranularitySpec.getTimeFormat());
      if (formatFromTimeSpec.getFormat().equals(_dateTimeFieldSpec.getFormat())) {
        return timeColumnValue;
      }

      long timeColumnValueMS = timeGranularitySpec.toMillis(timeColumnValue);
      DateTimeFormatSpec toFormat = new DateTimeFormatSpec(_dateTimeFieldSpec.getFormat());
      return toFormat.fromMillisToFormat(timeColumnValueMS, Object.class);
    }

    @Override
    public void rewind()
        throws IOException {
      _baseRecordReader.rewind();
    }

    @Override
    public Schema getSchema() {
      return _schema;
    }

    @Override
    public void close()
        throws IOException {
      _baseRecordReader.close();
    }
  }
}
