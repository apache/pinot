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
package com.linkedin.pinot.core.data.udfs;

import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import com.linkedin.pinot.core.operator.transform.transformer.datetime.BaseDateTimeTransformer;
import com.linkedin.pinot.core.operator.transform.transformer.datetime.DateTimeTransformerFactory;
public class DateTimeFunctions {
  private static ThreadLocal<long[]> longInputArrayCache = new ThreadLocal<long[]>(){
    protected long[] initialValue() {
      return new long[1];
    };
  };
  private static ThreadLocal<long[]> longOutputArrayCache = new ThreadLocal<long[]>(){
    protected long[] initialValue() {
      return new long[1];
    };
  };
  private static ThreadLocal<String[]> stringInputArrayCache = new ThreadLocal<String[]>(){
    protected String[] initialValue() {
      return new String[1];
    };
  };
  private static ThreadLocal<String[]> stringOutputArrayCache = new ThreadLocal<String[]>(){
    protected String[] initialValue() {
      return new String[1];
    };
  };
  
  public static DateTime parseSDF(String input, String format){
    return DateTimeFormat.forPattern(format).parseDateTime(input);
  }

  public static DateTime parseMillis(String input, String format){
    return DateTimeFormat.forPattern(format).parseDateTime(input);
  }

  public static DateTime bucketDateTime(DateTime dateTime, int timeUnitSize, String timeunit){
    long factor = TimeUnit.valueOf(timeunit).toMillis(timeUnitSize);
    return new DateTime( (dateTime.getMillis()/factor) * factor);
  }
  
  public static long toMillis(DateTime input){
    return input.getMillis();
  }
  
  public static String toSDF(DateTime input, String format){
    return DateTimeFormat.forPattern(format).print(input);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public static long sdf2epoch(String input, String inputFormatSpec, String outputFormatSpec, String outputGranularitySpec){
    BaseDateTimeTransformer dateTimeTransformer = getDateTimeTransformer(inputFormatSpec, outputFormatSpec, outputGranularitySpec);
    String[] stringInputArray = stringInputArrayCache.get();
    stringInputArray[0] = input;
    
    long[] longOutputArray = longOutputArrayCache.get();
    dateTimeTransformer.transform(stringInputArray, longOutputArray, 1);
    return longOutputArray[0];
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public static String sdf2sdf(String input, String inputFormatSpec, String outputFormatSpec, String outputGranularitySpec){
    BaseDateTimeTransformer dateTimeTransformer = getDateTimeTransformer(inputFormatSpec, outputFormatSpec, outputGranularitySpec);
    String[] stringInputArray = stringInputArrayCache.get();
    stringInputArray[0] = input;
    
    String[] stringOutputArray = stringOutputArrayCache.get();
    dateTimeTransformer.transform(stringInputArray, stringOutputArray, 1);
    return stringOutputArray[0];
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public static long epoch2epoch(long input, String inputFormatSpec, String outputFormatSpec, String outputGranularitySpec){
    BaseDateTimeTransformer dateTimeTransformer = getDateTimeTransformer(inputFormatSpec, outputFormatSpec, outputGranularitySpec);
    long[] longInputArray = longInputArrayCache.get();
    longInputArray[0] = input;
    
    long[] longOutputArray = longOutputArrayCache.get();
    dateTimeTransformer.transform(longInputArray, longOutputArray, 1);
    return longOutputArray[0];
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public static String epoch2sdf(long input, String inputFormatSpec, String outputFormatSpec, String outputGranularitySpec){
    BaseDateTimeTransformer dateTimeTransformer = getDateTimeTransformer(inputFormatSpec, outputFormatSpec, outputGranularitySpec);
    long[] longInputArray = longInputArrayCache.get();
    longInputArray[0] = input;
    String[] stringOutputArray = stringOutputArrayCache.get();
    dateTimeTransformer.transform(longInputArray  , stringOutputArray, 1);
    return stringOutputArray[0];
  }
  
  @SuppressWarnings("rawtypes")
  private static BaseDateTimeTransformer getDateTimeTransformer(String inputFormatSpec, String outputFormatSpec, String outputGranularitySpec) {
    return DateTimeTransformerFactory.getCachedDateTimeTransformer(inputFormatSpec, outputFormatSpec, outputGranularitySpec);
  }
}
