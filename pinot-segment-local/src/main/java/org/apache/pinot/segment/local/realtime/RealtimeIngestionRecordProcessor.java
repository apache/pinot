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
package org.apache.pinot.segment.local.realtime;

import org.apache.pinot.segment.local.segment.creator.TransformPipeline;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamDataDecoderResult;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.slf4j.Logger;


/// Applies the same record transformation, indexing and error policy to live consumption and fixed-range replay.
/// Stream fetching, decode retries, offsets and segment completion belong to the caller. Instances and their
/// listeners are thread-confined; callers must serialize processing, including a catch-up after live consumption.
public final class RealtimeIngestionRecordProcessor {
  private static final String STOP_ON_DECODE_ERROR_CONFIG = "stopOnDecodeError";

  private final TransformPipeline _transformPipeline;
  private final MutableSegment _segment;
  private final boolean _stopOnDecodeError;
  private final Listener _listener;
  private int _numRowsIndexed;

  public RealtimeIngestionRecordProcessor(TransformPipeline transformPipeline, MutableSegment segment,
      boolean stopOnDecodeError, Listener listener) {
    _transformPipeline = transformPipeline;
    _segment = segment;
    _stopOnDecodeError = stopOnDecodeError;
    _listener = listener;
  }

  /// Processes a decoded message after the caller has exhausted or completed any decode retries. Transform and
  /// indexing failures skip the affected row; a terminal decode failure stops only when configured to do so.
  /// Returns the last successful index operation's capacity signal, or true if no row was successfully indexed.
  public boolean process(StreamDataDecoderResult decodedRow, StreamMessageMetadata metadata) {
    _numRowsIndexed = 0;
    Exception decodeException = decodedRow.getException();
    if (decodeException != null) {
      _listener.onDecodeError(decodeException, metadata, _stopOnDecodeError);
      if (_stopOnDecodeError) {
        throw new RuntimeException("Stopping consumption due to decode error at offset: " + metadata.getOffset(),
            decodeException);
      }
      return true;
    }

    GenericRow row = decodedRow.getResult();
    TransformPipeline.Result result;
    try {
      result = _transformPipeline.processRow(row);
    } catch (Exception e) {
      _listener.onTransformError(row, metadata, e);
      return true;
    }
    _listener.onTransformResult(result, metadata);
    boolean canTakeMore = true;
    for (GenericRow transformedRow : result.getTransformedRows()) {
      try {
        canTakeMore = _segment.index(transformedRow, metadata);
        _numRowsIndexed++;
        _listener.onRowIndexed(transformedRow, metadata);
      } catch (Exception e) {
        _listener.onIndexError(transformedRow, metadata, e);
      }
    }
    return canTakeMore;
  }

  /// Number of successful index calls in the most recently processed message, without allocating a per-record result.
  public int getNumRowsIndexed() {
    return _numRowsIndexed;
  }

  /// Uses the live consumer's default and invalid-value behavior for both consumption drivers.
  public static boolean parseStopOnDecodeErrorConfig(StreamConfig streamConfig, Logger logger) {
    String configured = streamConfig.getStreamConfigsMap().get(STOP_ON_DECODE_ERROR_CONFIG);
    if (configured == null || "false".equalsIgnoreCase(configured)) {
      return false;
    }
    if ("true".equalsIgnoreCase(configured)) {
      return true;
    }
    logger.warn("Invalid value '{}' for configuration '{}'; "
        + "expected 'true' or 'false'. Defaulting to 'false'.", configured, STOP_ON_DECODE_ERROR_CONFIG);
    return false;
  }

  /// Reports processing outcomes to a driver's metrics and error reporting without depending on its lifecycle.
  public interface Listener {
    void onDecodeError(Exception exception, StreamMessageMetadata metadata, boolean fatal);

    void onTransformError(GenericRow row, StreamMessageMetadata metadata, Exception exception);

    void onIndexError(GenericRow row, StreamMessageMetadata metadata, Exception exception);

    default void onTransformResult(TransformPipeline.Result result, StreamMessageMetadata metadata) {
    }

    default void onRowIndexed(GenericRow row, StreamMessageMetadata metadata) {
    }
  }
}
