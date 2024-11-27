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
package org.apache.pinot.segment.local;

import java.util.List;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.testng.ITestResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


// Check for pinot buffer leaks after each test method
public interface PinotBuffersAfterMethodCheckRule {

  /*@BeforeClass
  default void enableBufferRecording() {
    PinotDataBuffer.enableBufferRecording();
  }*/

  @AfterMethod
  default void checkPinotBuffers(ITestResult result) {
    assertPinotBuffers(result);
  }

  @Test(enabled = false)
  static void assertPinotBuffers(Object target) {
    List<String> bufferInfos = PinotDataBuffer.getBufferInfo();
    if (bufferInfos.size() > 0) {

      StringBuilder builder;
      if (target instanceof ITestResult) {
        ITestResult result = (ITestResult) target;
        if (result.getStatus() != ITestResult.SUCCESS) {
          // don't override result of failed test
          PinotDataBuffer.closeOpenBuffers();
          return;
        }

        builder = new StringBuilder("Test method: ").append(result.getTestClass().getRealClass().getSimpleName())
            .append('.').append(result.getMethod().getMethodName());
      } else {
        builder = new StringBuilder("Test class: ").append(target);
      }

      StringBuilder message =
          builder.append(target)
              .append(" did not release ")
              .append(bufferInfos.size())
              .append(" pinot buffer(s): \n");
      for (String bufferInfo : bufferInfos) {
        message.append(bufferInfo).append('\n');
      }
      //close all open buffers otherwise one test will fail everything
      PinotDataBuffer.closeOpenBuffers();

      throw new AssertionError(message.toString());
    }
  }
}
