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
package org.apache.pinot.tools.streams;

import java.util.List;
import java.util.Properties;
import org.apache.pinot.spi.stream.StreamDataProducer;


/**
 * Represents one Pinot Real Time Data Source that can constantly generate data
 * For example it can be pulling a batch from Kafka, or polling some data via HTTP GET
 * The generator will be driven by PinotRealtimeSource to keep producing into some downstream sink
 */
public interface PinotSourceDataGenerator extends AutoCloseable {
  /**
   * Initialize the generator via a property file. It will be called at least once
   * @param properties the property files
   */
  void init(Properties properties);

  /**
   * Generate a small batch of rows represented by bytes.
   * It is up to the generator to define the binary format
   * @return a small list of RowWithKey, each element of the list will be written as one row of data
   */
  List<StreamDataProducer.RowWithKey> generateRows();
}
