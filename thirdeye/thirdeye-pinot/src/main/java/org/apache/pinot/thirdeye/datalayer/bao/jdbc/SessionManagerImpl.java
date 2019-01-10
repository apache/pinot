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
 */

package org.apache.pinot.thirdeye.datalayer.bao.jdbc;

import org.apache.pinot.thirdeye.datalayer.bao.SessionManager;
import org.apache.pinot.thirdeye.datalayer.dto.SessionDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.SessionBean;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;
import java.util.List;


public class SessionManagerImpl extends AbstractManagerImpl<SessionDTO> implements SessionManager {
  protected SessionManagerImpl() {
    super(SessionDTO.class, SessionBean.class);
  }

  @Override
  public SessionDTO findBySessionKey(String sessionKey) {
    List<SessionDTO> sessions = findByPredicate(Predicate.EQ("sessionKey", sessionKey));

    if (sessions.isEmpty()) {
      return null;
    }

    return sessions.get(0);
  }
}
