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
