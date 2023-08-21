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
package org.apache.pinot.segment.spi.evaluator;

import java.math.BigDecimal;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.spi.data.readers.Vector;


/**
 * This is an evolving SPI and subject to change.
 */
public interface TransformEvaluator {
  /**
   * Evaluate the JSON path and fill the value buffer
   * @param docIds the doc ids to evaluate the JSON path for
   * @param length the number of doc ids to evaluate for
   * @param reader the ForwardIndexReader
   * @param context the reader context
   * @param dictionary the dictionary
   * @param valueBuffer the values to fill
   * @param <T> type of the reader context
   */
  <T extends ForwardIndexReaderContext> void evaluateBlock(int[] docIds, int length, ForwardIndexReader<T> reader,
      T context, Dictionary dictionary, int[] dictIdBuffer, int[] valueBuffer);

  /**
   * Evaluate the JSON path and fill the value buffer
   * @param docIds the doc ids to evaluate the JSON path for
   * @param length the number of doc ids to evaluate for
   * @param reader the ForwardIndexReader
   * @param context the reader context
   * @param valueBuffer the values to fill
   * @param <T> type of the reader context
   */
  <T extends ForwardIndexReaderContext> void evaluateBlock(int[] docIds, int length, ForwardIndexReader<T> reader,
      T context, Dictionary dictionary, int[] dictIdBuffer, long[] valueBuffer);

  /**
   * Evaluate the JSON path and fill the value buffer
   * @param docIds the doc ids to evaluate the JSON path for
   * @param length the number of doc ids to evaluate for
   * @param reader the ForwardIndexReader
   * @param context the reader context
   * @param valueBuffer the values to fill
   * @param <T> type of the reader context
   */
  <T extends ForwardIndexReaderContext> void evaluateBlock(int[] docIds, int length, ForwardIndexReader<T> reader,
      T context, Dictionary dictionary, int[] dictIdBuffer, float[] valueBuffer);

  /**
   * Evaluate the JSON path and fill the value buffer
   * @param docIds the doc ids to evaluate the JSON path for
   * @param length the number of doc ids to evaluate for
   * @param reader the ForwardIndexReader
   * @param context the reader context
   * @param valueBuffer the values to fill
   * @param <T> type of the reader context
   */
  <T extends ForwardIndexReaderContext> void evaluateBlock(int[] docIds, int length, ForwardIndexReader<T> reader,
      T context, Dictionary dictionary, int[] dictIdBuffer, double[] valueBuffer);

  /**
   * Evaluate the JSON path and fill the value buffer
   * @param docIds the doc ids to evaluate the JSON path for
   * @param length the number of doc ids to evaluate for
   * @param reader the ForwardIndexReader
   * @param context the reader context
   * @param valueBuffer the values to fill
   * @param <T> type of the reader context
   */
  <T extends ForwardIndexReaderContext> void evaluateBlock(int[] docIds, int length, ForwardIndexReader<T> reader,
      T context, Dictionary dictionary, int[] dictIdBuffer, BigDecimal[] valueBuffer);

  /**
   * Evaluate the JSON path and fill the value buffer
   * @param docIds the doc ids to evaluate the JSON path for
   * @param length the number of doc ids to evaluate for
   * @param reader the ForwardIndexReader
   * @param context the reader context
   * @param valueBuffer the values to fill
   * @param <T> type of the reader context
   */
  <T extends ForwardIndexReaderContext> void evaluateBlock(int[] docIds, int length, ForwardIndexReader<T> reader,
      T context, Dictionary dictionary, int[] dictIdBuffer, Vector[] vector);

  /**
   * Evaluate the JSON path and fill the value buffer
   * @param docIds the doc ids to evaluate the JSON path for
   * @param length the number of doc ids to evaluate for
   * @param reader the ForwardIndexReader
   * @param context the reader context
   * @param valueBuffer the values to fill
   * @param <T> type of the reader context
   */
  <T extends ForwardIndexReaderContext> void evaluateBlock(int[] docIds, int length, ForwardIndexReader<T> reader,
      T context, Dictionary dictionary, int[] dictIdBuffer, String[] valueBuffer);

  /**
   * Evaluate the JSON path and fill the value buffer
   * @param docIds the doc ids to evaluate the JSON path for
   * @param length the number of doc ids to evaluate for
   * @param reader the ForwardIndexReader
   * @param context the reader context
   * @param dictIdsBuffer a buffer for dictionary ids if required
   * @param valueBuffer the values to fill
   * @param <T> type of the reader context
   */
  <T extends ForwardIndexReaderContext> void evaluateBlock(int[] docIds, int length,
      ForwardIndexReader<T> reader, T context, Dictionary dictionary, int[] dictIdsBuffer, int[][] valueBuffer);

  /**
   * Evaluate the JSON path and fill the value buffer
   * @param docIds the doc ids to evaluate the JSON path for
   * @param length the number of doc ids to evaluate for
   * @param reader the ForwardIndexReader
   * @param context the reader context
   * @param dictIdsBuffer a buffer for dictionary ids if required
   * @param valueBuffer the values to fill
   * @param <T> type of the reader context
   */
  <T extends ForwardIndexReaderContext> void evaluateBlock(int[] docIds, int length,
      ForwardIndexReader<T> reader, T context, Dictionary dictionary, int[] dictIdsBuffer, long[][] valueBuffer);

  /**
   * Evaluate the JSON path and fill the value buffer
   * @param docIds the doc ids to evaluate the JSON path for
   * @param length the number of doc ids to evaluate for
   * @param reader the ForwardIndexReader
   * @param context the reader context
   * @param dictIdsBuffer a buffer for dictionary ids if required
   * @param valueBuffer the values to fill
   * @param <T> type of the reader context
   */
  <T extends ForwardIndexReaderContext> void evaluateBlock(int[] docIds, int length,
      ForwardIndexReader<T> reader, T context, Dictionary dictionary, int[] dictIdsBuffer, float[][] valueBuffer);

  /**
   * Evaluate the JSON path and fill the value buffer
   * @param docIds the doc ids to evaluate the JSON path for
   * @param length the number of doc ids to evaluate for
   * @param reader the ForwardIndexReader
   * @param context the reader context
   * @param dictIdsBuffer a buffer for dictionary ids if required
   * @param valueBuffer the values to fill
   * @param <T> type of the reader context
   */
  <T extends ForwardIndexReaderContext> void evaluateBlock(int[] docIds, int length,
      ForwardIndexReader<T> reader, T context, Dictionary dictionary, int[] dictIdsBuffer, double[][] valueBuffer);

  /**
   * Evaluate the JSON path and fill the value buffer
   * @param docIds the doc ids to evaluate the JSON path for
   * @param length the number of doc ids to evaluate for
   * @param reader the ForwardIndexReader
   * @param context the reader context
   * @param dictIdsBuffer a buffer for dictionary ids if required
   * @param valueBuffer the values to fill
   * @param <T> type of the reader context
   */
  <T extends ForwardIndexReaderContext> void evaluateBlock(int[] docIds, int length,
      ForwardIndexReader<T> reader, T context, Dictionary dictionary, int[] dictIdsBuffer, String[][] valueBuffer);
}
