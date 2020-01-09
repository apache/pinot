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
package org.apache.pinot.spi.batch.ingestion.spec;

/**
 * ExecutionFrameworkSpec defines which ingestion jobs to be running.
 */
public class ExecutionFrameworkSpec {
  /**
   * The name of the execution framework, currently supports: Standalone.
   */
  private String _name;

  /**
   * The class implements org.apache.pinot.spi.batch.ingestion.runner.SegmentGenerationJobRunner interface.
   */
  private String _segmentGenerationJobRunnerClassName;

  /**
   * The class implements org.apache.pinot.spi.batch.ingestion.runner.SegmentTarPushJobRunner interface.
   */
  private String _segmentTarPushJobRunnerClassName;

  /**
   * The class implements org.apache.pinot.spi.batch.ingestion.runner.SegmentUriPushJobRunner interface.
   */
  private String _segmentUriPushJobRunnerClassName;

  public String getName() {
    return _name;
  }

  public void setName(String name) {
    _name = name;
  }

  public String getSegmentGenerationJobRunnerClassName() {
    return _segmentGenerationJobRunnerClassName;
  }

  public void setSegmentGenerationJobRunnerClassName(String segmentGenerationJobRunnerClassName) {
    _segmentGenerationJobRunnerClassName = segmentGenerationJobRunnerClassName;
  }

  public String getSegmentTarPushJobRunnerClassName() {
    return _segmentTarPushJobRunnerClassName;
  }

  public void setSegmentTarPushJobRunnerClassName(String segmentTarPushJobRunnerClassName) {
    _segmentTarPushJobRunnerClassName = segmentTarPushJobRunnerClassName;
  }

  public String getSegmentUriPushJobRunnerClassName() {
    return _segmentUriPushJobRunnerClassName;
  }

  public void setSegmentUriPushJobRunnerClassName(String segmentUriPushJobRunnerClassName) {
    _segmentUriPushJobRunnerClassName = segmentUriPushJobRunnerClassName;
  }
}
