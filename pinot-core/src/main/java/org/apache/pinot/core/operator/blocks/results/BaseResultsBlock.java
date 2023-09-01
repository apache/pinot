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
package org.apache.pinot.core.operator.blocks.results;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTable.MetadataKey;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Block;
import org.apache.pinot.core.query.request.context.QueryContext;


/**
 * The {@code BaseResultsBlock} class is the holder of the server side results.
 */
public abstract class BaseResultsBlock implements Block {
  private List<ProcessingException> _processingExceptions;
  private long _numTotalDocs;
  private long _numDocsScanned;
  private long _numEntriesScannedInFilter;
  private long _numEntriesScannedPostFilter;
  private int _numSegmentsProcessed;
  private int _numSegmentsMatched;
  private int _numConsumingSegmentsProcessed;
  private int _numConsumingSegmentsMatched;
  private long _executionThreadCpuTimeNs;
  private int _numServerThreads;

  @Nullable
  public List<ProcessingException> getProcessingExceptions() {
    return _processingExceptions;
  }

  public void setProcessingExceptions(List<ProcessingException> processingExceptions) {
    _processingExceptions = processingExceptions;
  }

  public void addToProcessingExceptions(ProcessingException processingException) {
    if (_processingExceptions == null) {
      _processingExceptions = new ArrayList<>();
    }
    _processingExceptions.add(processingException);
  }

  @VisibleForTesting
  public long getNumTotalDocs() {
    return _numTotalDocs;
  }

  public void setNumTotalDocs(long numTotalDocs) {
    _numTotalDocs = numTotalDocs;
  }

  @VisibleForTesting
  public long getNumDocsScanned() {
    return _numDocsScanned;
  }

  public void setNumDocsScanned(long numDocsScanned) {
    _numDocsScanned = numDocsScanned;
  }

  @VisibleForTesting
  public long getNumEntriesScannedInFilter() {
    return _numEntriesScannedInFilter;
  }

  public void setNumEntriesScannedInFilter(long numEntriesScannedInFilter) {
    _numEntriesScannedInFilter = numEntriesScannedInFilter;
  }

  @VisibleForTesting
  public long getNumEntriesScannedPostFilter() {
    return _numEntriesScannedPostFilter;
  }

  public void setNumEntriesScannedPostFilter(long numEntriesScannedPostFilter) {
    _numEntriesScannedPostFilter = numEntriesScannedPostFilter;
  }

  @VisibleForTesting
  public int getNumSegmentsProcessed() {
    return _numSegmentsProcessed;
  }

  public void setNumSegmentsProcessed(int numSegmentsProcessed) {
    _numSegmentsProcessed = numSegmentsProcessed;
  }

  @VisibleForTesting
  public int getNumSegmentsMatched() {
    return _numSegmentsMatched;
  }

  public void setNumSegmentsMatched(int numSegmentsMatched) {
    _numSegmentsMatched = numSegmentsMatched;
  }

  @VisibleForTesting
  public int getNumConsumingSegmentsProcessed() {
    return _numConsumingSegmentsProcessed;
  }

  public void setNumConsumingSegmentsProcessed(int numConsumingSegmentsProcessed) {
    _numConsumingSegmentsProcessed = numConsumingSegmentsProcessed;
  }

  @VisibleForTesting
  public int getNumConsumingSegmentsMatched() {
    return _numConsumingSegmentsMatched;
  }

  public void setNumConsumingSegmentsMatched(int numConsumingSegmentsMatched) {
    _numConsumingSegmentsMatched = numConsumingSegmentsMatched;
  }

  public long getExecutionThreadCpuTimeNs() {
    return _executionThreadCpuTimeNs;
  }

  public void setExecutionThreadCpuTimeNs(long executionThreadCpuTimeNs) {
    _executionThreadCpuTimeNs = executionThreadCpuTimeNs;
  }

  public int getNumServerThreads() {
    return _numServerThreads;
  }

  public void setNumServerThreads(int numServerThreads) {
    _numServerThreads = numServerThreads;
  }

  /**
   * Returns the total size (number of rows) in this result block, without having to materialize the rows.
   *
   * @see BaseResultsBlock#getRows()
   */
  public abstract int getNumRows();

  /**
   * Returns the query for the results. Return {@code null} when the block only contains metadata.
   */
  @Nullable
  public abstract QueryContext getQueryContext();

  /**
   * Returns the data schema for the results. Return {@code null} when the block only contains metadata.
   */
  @Nullable
  public abstract DataSchema getDataSchema();

  /**
   * Returns the rows for the results. Return {@code null} when the block only contains metadata.
   */
  @Nullable
  public abstract List<Object[]> getRows();

  /**
   * Returns a data table without metadata or exception attached.
   */
  public abstract DataTable getDataTable()
      throws IOException;

  /**
   * Returns the metadata for the results.
   */
  public Map<String, String> getResultsMetadata() {
    Map<String, String> metadata = new HashMap<>();
    metadata.put(MetadataKey.TOTAL_DOCS.getName(), Long.toString(_numTotalDocs));
    metadata.put(MetadataKey.NUM_DOCS_SCANNED.getName(), Long.toString(_numDocsScanned));
    metadata.put(MetadataKey.NUM_ENTRIES_SCANNED_IN_FILTER.getName(), Long.toString(_numEntriesScannedInFilter));
    metadata.put(MetadataKey.NUM_ENTRIES_SCANNED_POST_FILTER.getName(), Long.toString(_numEntriesScannedPostFilter));
    metadata.put(MetadataKey.NUM_SEGMENTS_PROCESSED.getName(), Integer.toString(_numSegmentsProcessed));
    metadata.put(MetadataKey.NUM_SEGMENTS_MATCHED.getName(), Integer.toString(_numSegmentsMatched));
    metadata.put(MetadataKey.NUM_CONSUMING_SEGMENTS_PROCESSED.getName(),
        Integer.toString(_numConsumingSegmentsProcessed));
    metadata.put(MetadataKey.NUM_CONSUMING_SEGMENTS_MATCHED.getName(), Integer.toString(_numConsumingSegmentsMatched));
    return metadata;
  }
}
