/*
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
 *
 */

package org.apache.pinot.thirdeye.model.download;

import java.nio.file.Path;
import java.util.Map;


/**
 * The model downloader interface. It downloads model files (e.x., trained tensorflow models), into a local path.
 * The implementation of this class can be configured to run at a certain frequency in ThirdEye server, so that the
 * models can be kept up-to-date.
 */
public abstract class ModelDownloader {
  private final Map<String, Object> properties;

  /**
   * Create a model downloader.
   * @param properties the properties
   */
  public ModelDownloader(Map<String, Object> properties) {
    this.properties = properties;
  }

  /**
   * fetch the models into the local path.
   * @param destination the destination path
   */
  public abstract void fetchModel(Path destination);
}
