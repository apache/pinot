package com.linkedin.thirdeye.datalayer.bao;

import java.util.List;

import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;


public interface EmailConfigurationManager extends AbstractManager<EmailConfigurationDTO>{

  List<EmailConfigurationDTO> findByFunctionId(Long id);

}
