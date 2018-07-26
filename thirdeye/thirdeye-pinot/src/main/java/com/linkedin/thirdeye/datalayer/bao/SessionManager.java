package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.dto.SessionDTO;


public interface SessionManager extends AbstractManager<SessionDTO> {

  SessionDTO findBySessionKey(String sessionKey);

}
