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
package org.apache.pinot.common.datablock;

import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A block type to indicate some metadata about the current processing state.
 * For the different types of metadata blocks see {@link MetadataBlockType}.
 */
public class MetadataBlock extends BaseDataBlock {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataBlock.class);
  @VisibleForTesting
  static final int VERSION = 2;
  @Nullable
  private List<DataBuffer> _statsByStage;

  private MetadataBlock() {
    this(Collections.emptyList());
  }

  public static MetadataBlock newEos() {
    return new MetadataBlock();
  }

  public static MetadataBlock newError(Map<Integer, String> exceptions) {
    MetadataBlock errorBlock = new MetadataBlock();
    for (Map.Entry<Integer, String> exception : exceptions.entrySet()) {
      errorBlock.addException(exception.getKey(), exception.getValue());
    }
    return errorBlock;
  }

  public static MetadataBlock newErrorWithStats(Map<Integer, String> exceptions, List<DataBuffer> statsByStage) {
    MetadataBlock errorBlock = newError(exceptions);
    errorBlock._statsByStage = statsByStage;
    return errorBlock;
  }

  public static MetadataBlock newEosWithStats(List<DataBuffer> statsByStage) {
    return new MetadataBlock(statsByStage);
  }

  public MetadataBlock(List<DataBuffer> statsByStage) {
    super(0, null, new String[0], new byte[0], new byte[0]);
    _statsByStage = statsByStage;
  }

  public MetadataBlockType getType() {
    return _errCodeToExceptionMap.isEmpty() ? MetadataBlockType.EOS : MetadataBlockType.ERROR;
  }

  /**
   * Returns the list of serialized stats.
   * <p>
   * The returned list may contain nulls, which would mean that no stats were available for that stage.
   */
  @Nullable
  @Override
  public List<DataBuffer> getStatsByStage() {
    return _statsByStage;
  }

  @Override
  public Type getDataBlockType() {
    return Type.METADATA;
  }

  @Override
  protected int getOffsetInFixedBuffer(int rowId, int colId) {
    throw new UnsupportedOperationException("Not supported in metadata block");
  }

  @Override
  protected int getFixDataSize() {
    return 0;
  }

  public enum MetadataBlockType {
    /**
     * Indicates that this block is the final block to be sent
     * (End Of Stream) as part of an operator chain computation.
     */
    EOS,

    /**
     * An {@code ERROR} metadata block indicates that there was
     * some error during computation. To retrieve the error that
     * occurred, use {@link MetadataBlock#getExceptions()}
     */
    ERROR
  }
}
