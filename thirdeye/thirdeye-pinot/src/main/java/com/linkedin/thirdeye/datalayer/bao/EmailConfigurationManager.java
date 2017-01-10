package com.linkedin.thirdeye.datalayer.bao;

import java.util.List;

import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;

@Deprecated
public interface EmailConfigurationManager extends AbstractManager<EmailConfigurationDTO> {

  List<EmailConfigurationDTO> findByFunctionId(Long id);

  List<EmailConfigurationDTO> findByCollectionMetric(String collection, String metric);

  List<EmailConfigurationDTO> findByCollection(String collection);
}
