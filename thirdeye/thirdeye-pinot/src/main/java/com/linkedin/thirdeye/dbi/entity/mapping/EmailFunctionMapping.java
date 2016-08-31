package com.linkedin.thirdeye.dbi.entity.mapping;

import com.linkedin.thirdeye.dbi.entity.base.AbstractMappingEntity;
import com.linkedin.thirdeye.dbi.entity.data.AnomalyFunction;
import com.linkedin.thirdeye.dbi.entity.data.EmailConfiguration;
import java.util.List;

public class EmailFunctionMapping
    extends AbstractMappingEntity<EmailConfiguration, AnomalyFunction> {

  public EmailFunctionMapping(List<Long> emailConfigIds, List<Long> anomalyFunctionIds) {
    super(emailConfigIds, anomalyFunctionIds);
  }
}
