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
package org.apache.pinot.query.routing;

import com.google.protobuf.ByteString;
import java.util.List;


/**
 * {@code SharedMailboxInfos} is the shared version of the {@link MailboxInfos} which can cache the proto bytes and
 * reduce overhead of serialization.
 */
public class SharedMailboxInfos extends MailboxInfos {
  private ByteString _protoBytes;

  public SharedMailboxInfos(List<MailboxInfo> mailboxInfos) {
    super(mailboxInfos);
  }

  public SharedMailboxInfos(MailboxInfo mailboxInfo) {
    super(mailboxInfo);
  }

  @Override
  public synchronized ByteString toProtoBytes() {
    if (_protoBytes == null) {
      _protoBytes = super.toProtoBytes();
    }
    return _protoBytes;
  }
}
