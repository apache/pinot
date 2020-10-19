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
import java.util.List;


/**
 * PRODUCE
 *   Produce events onto the stream, and verify that the number of rows in the tables increased
 *   by the number of rows produced. Also, verify the segment state for all replicas of the tables
 *
 * TODO: Consider using a file-based stream, where "pushing" events is simply adding new files to
 *       a folder named after the "stream". The implementation for the consumer would need to watch
 *       for new files and read them out. There could be one sub-folder per partition. This approach
 *       can save us handling kafka errors, etc.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamOp extends BaseOp {
  public enum Op {
    PRODUCE
  }

  private String _streamConfigFileName;
  private int _numRows;
  private String _inputDataFileName;
  private List<String> _tableConfigFileNames;

  public String getStreamConfigFileName() {
    return _streamConfigFileName;
  }

  public StreamOp() {
    super(OpType.STREAM_OP);
  }

  public void setStreamConfigFileName(String streamConfigFileName) {
    _streamConfigFileName = streamConfigFileName;
  }

  public int getNumRows() {
    return _numRows;
  }

  public void setNumRows(int numRows) {
    _numRows = numRows;
  }

  public String getInputDataFileName() {
    return _inputDataFileName;
  }

  public void setInputDataFileName(String inputDataFileName) {
    _inputDataFileName = inputDataFileName;
  }

  @Override
  boolean runOp() {
    System.out.println("Produce rows into stream " + _streamConfigFileName + " and verify rows in tables "
        + _tableConfigFileNames);
    return true;
  }

  public List<String> getTableConfigFileNames() {
    return _tableConfigFileNames;
  }

  public void setTableConfigFileNames(List<String> tableConfigFileNames) {
    _tableConfigFileNames = tableConfigFileNames;
  }
}
