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
package org.apache.pinot.query.runtime.plan.serde;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.apache.pinot.query.planner.nodes.StageNode;


public class StageNodeSerDeUtils {
  private StageNodeSerDeUtils() {
    // do not instantiate.
  }

  public static StageNode deserializeStageRoot(ByteString serializeStagePlan) {
    try (ByteArrayInputStream bs = new ByteArrayInputStream(serializeStagePlan.toByteArray());
        ObjectInputStream is = new ObjectInputStream(bs)) {
      Object o = is.readObject();
      Preconditions.checkState(o instanceof StageNode, "invalid worker query request object");
      return (StageNode) o;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static ByteString serializeStageRoot(StageNode stageRoot) {
    try (ByteArrayOutputStream bs = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(bs)) {
      os.writeObject(stageRoot);
      return ByteString.copyFrom(bs.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
