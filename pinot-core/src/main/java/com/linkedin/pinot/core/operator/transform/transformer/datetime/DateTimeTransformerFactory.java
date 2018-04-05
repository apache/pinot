/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator.transform.transformer.datetime;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.linkedin.pinot.common.data.DateTimeFieldSpec.TimeFormat;
import com.linkedin.pinot.common.data.DateTimeFormatSpec;
import com.linkedin.pinot.common.data.DateTimeGranularitySpec;

@SuppressWarnings("rawtypes")
public class DateTimeTransformerFactory {

  static Map<String, Map<String, Map<String, ThreadLocal<BaseDateTimeTransformer>>>> cache = new ConcurrentHashMap<>();

  private DateTimeTransformerFactory() {

  }

  /**
   * Always creates a new transformer. Typically used in query execution. At some point, we can use cache here as well once we have a way expire entries from
   * the cache based on last time used
   * 
   * @param inputFormatStr
   * @param outputFormatStr
   * @param outputGranularityStr
   * @return
   */
  public static BaseDateTimeTransformer getDateTimeTransformer(String inputFormatStr, String outputFormatStr, String outputGranularityStr) {
    return createTransformer(inputFormatStr, outputFormatStr, outputGranularityStr);
  }

  /**
   * Creates & Caches the transformers.Used during ingestion or segment generation.
   * 
   * @param inputFormatStr
   * @param outputFormatStr
   * @param outputGranularityStr
   * @return
   */
  public static BaseDateTimeTransformer getCachedDateTimeTransformer(String inputFormatStr, String outputFormatStr, String outputGranularityStr) {

    // check the cache
    Map<String, Map<String, ThreadLocal<BaseDateTimeTransformer>>> inputFormatMap;
    inputFormatMap = cache.get(inputFormatStr);
    if (inputFormatMap != null) {
      Map<String, ThreadLocal<BaseDateTimeTransformer>> map = inputFormatMap.get(outputFormatStr);
      if (map != null) {
        ThreadLocal<BaseDateTimeTransformer> transformerCache = map.get(outputGranularityStr);
        if (transformerCache != null) {
          return transformerCache.get();
        }
      }
    }
    // create the transformer cache.
    ThreadLocal<BaseDateTimeTransformer> transformerCache = createAndGetTransformerCache(inputFormatStr, outputFormatStr, outputGranularityStr);
    return transformerCache.get();
  }

  private static BaseDateTimeTransformer createTransformer(String inputFormatStr, String outputFormatStr, String outputGranularityStr) {
    BaseDateTimeTransformer transformer;
    DateTimeFormatSpec inputFormat = new DateTimeFormatSpec(inputFormatStr);
    DateTimeFormatSpec outputFormat = new DateTimeFormatSpec(outputFormatStr);
    DateTimeGranularitySpec outputGranularity = new DateTimeGranularitySpec(outputGranularityStr);

    TimeFormat inputTimeFormat = inputFormat.getTimeFormat();
    TimeFormat outputTimeFormat = outputFormat.getTimeFormat();
    if (inputTimeFormat == TimeFormat.EPOCH) {
      if (outputTimeFormat == TimeFormat.EPOCH) {
        transformer = new EpochToEpochTransformer(inputFormat, outputFormat, outputGranularity);
      } else {
        transformer = new EpochToSDFTransformer(inputFormat, outputFormat, outputGranularity);
      }
    } else {
      if (outputTimeFormat == TimeFormat.EPOCH) {
        transformer = new SDFToEpochTransformer(inputFormat, outputFormat, outputGranularity);
      } else {
        transformer = new SDFToSDFTransformer(inputFormat, outputFormat, outputGranularity);
      }
    }
    return transformer;
  }

  private static ThreadLocal<BaseDateTimeTransformer> createAndGetTransformerCache(final String inputFormatStr, final String outputFormatStr,
      final String outputGranularityStr) {
    ThreadLocal<BaseDateTimeTransformer> transformerCache;
    synchronized (cache) {
      Map<String, Map<String, ThreadLocal<BaseDateTimeTransformer>>> inputFormatMap = cache.get(inputFormatStr);
      if (inputFormatMap == null) {
        inputFormatMap = new ConcurrentHashMap<>();
        cache.put(inputFormatStr, inputFormatMap);
      }
      Map<String, ThreadLocal<BaseDateTimeTransformer>> outputFormatMap = inputFormatMap.get(outputFormatStr);
      if (outputFormatMap == null) {
        outputFormatMap = new ConcurrentHashMap<>();
        inputFormatMap.put(outputFormatStr, outputFormatMap);
      }
      transformerCache = outputFormatMap.get(outputGranularityStr);
      if (transformerCache != null) {
        transformerCache = new ThreadLocal<BaseDateTimeTransformer>() {
          protected BaseDateTimeTransformer initialValue() {
            return createTransformer(inputFormatStr, outputFormatStr, outputGranularityStr);
          }
        };
      }
      outputFormatMap.put(outputGranularityStr, transformerCache);
      return transformerCache;
    }
  }
}
