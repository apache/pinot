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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.core.data.readers.PinotSegmentRecordReader;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.core.segment.store.ColumnIndexType;
import org.apache.pinot.core.segment.store.SegmentDirectory;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>SegmentPurger</code> class takes a segment and purges/modifies its records and generate a new segment with
 * the remaining modified records.
 */
public class SegmentPurger {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentPurger.class);

  private final String _rawTableName;
  private final File _originalIndexDir;
  private final File _workingDir;
  private final RecordPurger _recordPurger;
  private final RecordModifier _recordModifier;

  private int _numRecordsPurged;
  private int _numRecordsModified;

  public SegmentPurger(String rawTableName, File originalIndexDir, File workingDir, @Nullable RecordPurger recordPurger,
      @Nullable RecordModifier recordModifier) {
    Preconditions.checkArgument(recordPurger != null || recordModifier != null,
        "At least one of record purger and modifier should be non-null");
    _rawTableName = rawTableName;
    _originalIndexDir = originalIndexDir;
    _workingDir = workingDir;
    _recordPurger = recordPurger;
    _recordModifier = recordModifier;
  }

  public File purgeSegment()
      throws Exception {
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(_originalIndexDir);
    String segmentName = segmentMetadata.getName();
    LOGGER.info("Start purging table: {}, segment: {}", _rawTableName, segmentName);

    try (PurgeRecordReader purgeRecordReader = new PurgeRecordReader()) {
      // Make a first pass through the data to see if records need to be purged or modified
      while (purgeRecordReader.hasNext()) {
        purgeRecordReader.next();
      }

      if (_numRecordsModified == 0 && _numRecordsPurged == 0) {
        // Returns null if no records to be modified or purged
        return null;
      }

      Schema schema = purgeRecordReader.getSchema();
      // FIXME: figure out how to get table config here.
      //  Fine for now, since we will only have timeFieldSpec.
      //  Will be an issue once we start using DateTimeFieldSpec
      SegmentGeneratorConfig config = new SegmentGeneratorConfig(null, schema);
      config.setOutDir(_workingDir.getPath());
      config.setTableName(_rawTableName);
      config.setSegmentName(segmentName);

      // Keep index creation time the same as original segment because both segments use the same raw data.
      // This way, for REFRESH case, when new segment gets pushed to controller, we can use index creation time to
      // identify if the new pushed segment has newer data than the existing one.
      config.setCreationTime(String.valueOf(segmentMetadata.getIndexCreationTime()));

      // The time column type info is not stored in the segment metadata.
      // Keep segment start/end time to properly handle time column type other than EPOCH (e.g.SIMPLE_FORMAT).
      if (segmentMetadata.getTimeInterval() != null) {
        config.setTimeColumnName(segmentMetadata.getTimeColumn());
        config.setStartTime(Long.toString(segmentMetadata.getStartTime()));
        config.setEndTime(Long.toString(segmentMetadata.getEndTime()));
        config.setSegmentTimeUnit(segmentMetadata.getTimeUnit());
      }

      // Generate inverted index if it exists in the original segment
      // TODO: once the column metadata correctly reflects whether inverted index exists for the column, use that
      //       instead of reading the segment
      // TODO: uniform the behavior of Pinot Hadoop segment generation, segment converter and purger
      List<String> invertedIndexCreationColumns = new ArrayList<>();
      try (SegmentDirectory segmentDirectory = SegmentDirectory
          .createFromLocalFS(_originalIndexDir, segmentMetadata, ReadMode.mmap);
          SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
        for (String column : schema.getColumnNames()) {
          if (reader.hasIndexFor(column, ColumnIndexType.INVERTED_INDEX)) {
            invertedIndexCreationColumns.add(column);
          }
        }
      }
      config.setInvertedIndexCreationColumns(invertedIndexCreationColumns);

      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      purgeRecordReader.rewind();
      driver.init(config, purgeRecordReader);
      driver.build();
    }

    LOGGER.info("Finish purging table: {}, segment: {}, purged {} records, modified {} records", _rawTableName,
        segmentName, _numRecordsPurged, _numRecordsModified);
    return new File(_workingDir, segmentName);
  }

  public RecordPurger getRecordPurger() {
    return _recordPurger;
  }

  public RecordModifier getRecordModifier() {
    return _recordModifier;
  }

  public int getNumRecordsPurged() {
    return _numRecordsPurged;
  }

  public int getNumRecordsModified() {
    return _numRecordsModified;
  }

  private class PurgeRecordReader implements RecordReader {
    final PinotSegmentRecordReader _pinotSegmentRecordReader;

    // Reusable generic row to store the next row to return
    GenericRow _nextRow = new GenericRow();
    // Flag to mark whether we need to fetch another row
    boolean _nextRowReturned = true;
    // Flag to mark whether all records have been iterated
    boolean _finished = false;

    PurgeRecordReader()
        throws Exception {
      _pinotSegmentRecordReader = new PinotSegmentRecordReader(_originalIndexDir);
    }

    public Schema getSchema() {
      return _pinotSegmentRecordReader.getSchema();
    }

    @Override
    public void init(File dataFile, Set<String> fieldsToRead, @Nullable RecordReaderConfig recordReaderConfig) {
    }

    @Override
    public boolean hasNext() {
      if (_recordPurger == null) {
        return _pinotSegmentRecordReader.hasNext();
      } else {
        // If all records have already been iterated, return false
        if (_finished) {
          return false;
        }

        // If next row has not been returned, return true
        if (!_nextRowReturned) {
          return true;
        }

        // Try to get the next row to return
        while (_pinotSegmentRecordReader.hasNext()) {
          _nextRow = _pinotSegmentRecordReader.next(_nextRow);

          if (_recordPurger.shouldPurge(_nextRow)) {
            _numRecordsPurged++;
          } else {
            _nextRowReturned = false;
            return true;
          }
        }

        // Cannot find next row to return, return false
        _finished = true;
        return false;
      }
    }

    @Override
    public GenericRow next() {
      return next(new GenericRow());
    }

    @Override
    public GenericRow next(GenericRow reuse) {
      if (_recordPurger == null) {
        reuse = _pinotSegmentRecordReader.next(reuse);
      } else {
        Preconditions.checkState(!_nextRowReturned);
        reuse.init(_nextRow);
        _nextRowReturned = true;
      }

      if (_recordModifier != null) {
        if (_recordModifier.modifyRecord(reuse)) {
          _numRecordsModified++;
        }
      }

      return reuse;
    }

    @Override
    public void rewind() {
      _pinotSegmentRecordReader.rewind();
      _nextRowReturned = true;
      _finished = false;

      _numRecordsPurged = 0;
      _numRecordsModified = 0;
    }

    @Override
    public void close()
        throws IOException {
      _pinotSegmentRecordReader.close();
    }
  }

  /**
   * Factory for {@link RecordPurger}
   */
  public interface RecordPurgerFactory {

    /**
     * Get the {@link RecordPurger} for the given table.
     */
    RecordPurger getRecordPurger(String rawTableName);
  }

  /**
   * Purger for each {@link GenericRow} record.
   */
  public interface RecordPurger {

    /**
     * Return <code>true</code> if the record should be purged.
     */
    boolean shouldPurge(GenericRow row);
  }

  /**
   * Factory for {@link RecordModifier}
   */
  public interface RecordModifierFactory {

    /**
     * Get the {@link RecordModifier} for the given table.
     */
    RecordModifier getRecordModifier(String rawTableName);
  }

  /**
   * Modifier for each {@link GenericRow} record.
   */
  public interface RecordModifier {

    /**
     * Modify the record inplace, and return <code>true</code> if the record get modified.
     */
    boolean modifyRecord(GenericRow row);
  }
}
