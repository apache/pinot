/*
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.thirdeye.detection;

import com.linkedin.thirdeye.detection.spi.model.InputData;
import com.linkedin.thirdeye.detection.spi.model.InputDataSpec;

/**
 * Input data fetcher interface.
 * For components to fetch the input data it need.
 */

public interface InputDataFetcher {
  /**
   * fetch data for input data spec
   */
  InputData fetchData(InputDataSpec inputDataSpec);
}
