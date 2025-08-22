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
package org.apache.pinot.plugin.metrics.compound;

import java.util.List;
import org.apache.pinot.spi.metrics.PinotMetricReporter;


/**
 * CompoundPinotMetricReporter delegates metric reporting to each of the reporters in the list.
 */
public class CompoundPinotMetricReporter implements PinotMetricReporter {
  private final List<PinotMetricReporter> _reporters;

  public CompoundPinotMetricReporter(List<PinotMetricReporter> reporters) {
    _reporters = reporters;
  }

  @Override
  public void start() {
    for (PinotMetricReporter reporter : _reporters) {
      reporter.start();
    }
  }
}
