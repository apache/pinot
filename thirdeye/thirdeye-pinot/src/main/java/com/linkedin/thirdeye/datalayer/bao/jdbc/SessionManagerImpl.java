/**
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

package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.linkedin.thirdeye.datalayer.bao.SessionManager;
import com.linkedin.thirdeye.datalayer.dto.SessionDTO;
import com.linkedin.thirdeye.datalayer.pojo.SessionBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;
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
