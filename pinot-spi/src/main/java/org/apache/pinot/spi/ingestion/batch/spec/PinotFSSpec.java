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
package org.apache.pinot.spi.ingestion.batch.spec;

import java.io.Serializable;
import java.util.Map;


/**
 * PinotFSSpec defines how to initialize a PinotFS for given scheme.
 *
 */
public class PinotFSSpec implements Serializable {

  /**
   * Scheme used to identify a PinotFS.
   * E.g. local, hdfs, dbfs, etc
   */
  private String _scheme;

  /**
   * Class name used to create the PinotFS instance.
   * E.g.
   *    org.apache.pinot.spi.filesystem.LocalPinotFS is used for local filesystem
   *    org.apache.pinot.plugin.filesystem.AzurePinotFS is used for Azure Data Lake
   *    org.apache.pinot.plugin.filesystem.HadoopPinotFS is used for HDFS
   */
  private String _className;

  /**
   * Configs used to init the PinotFS instances.
   */
  private Map<String, String> _configs;

  public String getScheme() {
    return _scheme;
  }

  /**
   * Scheme used to identify a Pinot FileSystem. It should match the scheme in the file uri for access.
   * E.g. local, hdfs, dbfs, etc
   *
   * @param scheme
   */
  public void setScheme(String scheme) {
    _scheme = scheme;
  }

  public String getClassName() {
    return _className;
  }

  /**
   * Class name used to create the PinotFS instance.
   * E.g.
   *    org.apache.pinot.spi.filesystem.LocalPinotFS is used for local filesystem
   *    org.apache.pinot.plugin.filesystem.AzurePinotFS is used for Azure Data Lake
   *    org.apache.pinot.plugin.filesystem.HadoopPinotFS is used for HDFS
   *
   * @param className
   */
  public void setClassName(String className) {
    _className = className;
  }

  public Map<String, String> getConfigs() {
    return _configs;
  }

  public void setConfigs(Map<String, String> configs) {
    _configs = configs;
  }
}
