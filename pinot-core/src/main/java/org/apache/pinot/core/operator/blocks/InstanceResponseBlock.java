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
package org.apache.pinot.core.operator.blocks;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Block;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;


/**
 * The {@code InstanceResponseBlock} is the holder of the server side results.
 */
public class InstanceResponseBlock implements Block {
  private final BaseResultsBlock _resultsBlock;
  private final Map<Integer, String> _exceptions;
  private final Map<String, String> _metadata;

  public InstanceResponseBlock(BaseResultsBlock resultsBlock) {
    _resultsBlock = resultsBlock;
    _exceptions = new HashMap<>();
    List<ProcessingException> processingExceptions = resultsBlock.getProcessingExceptions();
    if (processingExceptions != null) {
      for (ProcessingException processingException : processingExceptions) {
        _exceptions.put(processingException.getErrorCode(), processingException.getMessage());
      }
    }
    _metadata = resultsBlock.getResultsMetadata();
  }

  /**
   * Metadata only instance response.
   */
  public InstanceResponseBlock() {
    _resultsBlock = null;
    _exceptions = new HashMap<>();
    _metadata = new HashMap<>();
  }

  private InstanceResponseBlock(Map<Integer, String> exceptions, Map<String, String> metadata) {
    _resultsBlock = null;
    _exceptions = exceptions;
    _metadata = metadata;
  }

  public InstanceResponseBlock toMetadataOnlyResponseBlock() {
    return new InstanceResponseBlock(_exceptions, _metadata);
  }

  public void addException(ProcessingException processingException) {
    _exceptions.put(processingException.getErrorCode(), processingException.getMessage());
  }

  public void addException(int errorCode, String exceptionMessage) {
    _exceptions.put(errorCode, exceptionMessage);
  }

  public void addMetadata(String key, String value) {
    _metadata.put(key, value);
  }

  @Nullable
  public BaseResultsBlock getResultsBlock() {
    return _resultsBlock;
  }

  public Map<Integer, String> getExceptions() {
    return _exceptions;
  }

  public Map<String, String> getResponseMetadata() {
    return _metadata;
  }

  @Nullable
  public DataSchema getDataSchema() {
    return _resultsBlock != null ? _resultsBlock.getDataSchema() : null;
  }

  @Nullable
  public List<Object[]> getRows() {
    return _resultsBlock != null ? _resultsBlock.getRows() : null;
  }

  public DataTable toDataTable()
      throws IOException {
    DataTable dataTable = toDataOnlyDataTable();
    attachMetadata(dataTable);
    return dataTable;
  }

  public DataTable toDataOnlyDataTable()
      throws IOException {
    return _resultsBlock != null ? _resultsBlock.getDataTable() : DataTableBuilderFactory.getEmptyDataTable();
  }

  public DataTable toMetadataOnlyDataTable() {
    DataTable dataTable = DataTableBuilderFactory.getEmptyDataTable();
    attachMetadata(dataTable);
    return dataTable;
  }

  private void attachMetadata(DataTable dataTable) {
    for (Map.Entry<Integer, String> entry : _exceptions.entrySet()) {
      dataTable.addException(entry.getKey(), entry.getValue());
    }
    dataTable.getMetadata().putAll(_metadata);
  }
}
