package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.dto.ApplicationDTO;
import java.util.List;


public interface ApplicationManager extends AbstractManager<ApplicationDTO> {
  List<ApplicationDTO> findByName(String name);
}
