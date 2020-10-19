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
package org.apache.pinot.compat.tests;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;


/**
 * Segment Operations:
 * UPLOAD:
 *   Generates a segment for a table from the data in the input file.
 *   Uploads the segment, and verifies that the segments appear in externalview
 * DELETE:
 *   Deletes the segment from the table.
 *
 * TODO:
 *  - Maybe segment names can be auto-generated if the name is "AUTO".
 *  - We can add segmentGeneration config file as an option also
 *  - We can consider supporting different readers, starting with csv. Will help in easily scanning the data.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SegmentOp extends BaseOp {
  public enum Op {
    UPLOAD,
    DELETE
  }

  private Op _op;
  private String _inputDataFileName;
  private String _segmentName;
  private String _tableConfigFileName;

  public SegmentOp() {
    super(OpType.SEGMENT_OP);
  }

  public Op getOp() {
    return _op;
  }

  public void setOp(Op op) {
    _op = op;
  }

  public String getInputDataFileName() {
    return _inputDataFileName;
  }

  public void setInputDataFileName(String inputDataFileName) {
    _inputDataFileName = inputDataFileName;
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public void setSegmentName(String segmentName) {
    _segmentName = segmentName;
  }

  public String getTableConfigFileName() {
    return _tableConfigFileName;
  }

  public void setTableConfigFileName(String tableConfigFileName) {
    _tableConfigFileName = tableConfigFileName;
  }

  @Override
  boolean runOp() {
    switch(_op) {
      case UPLOAD:
        System.out.println("Generating segment " + _segmentName + " from " + _inputDataFileName + " and uploading to " +
            _tableConfigFileName);
      case DELETE:
    }
    return true;
  }
}
