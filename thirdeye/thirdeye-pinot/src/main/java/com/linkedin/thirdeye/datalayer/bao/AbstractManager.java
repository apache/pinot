package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.dto.AbstractDTO;
import com.linkedin.thirdeye.datalayer.entity.AbstractEntity;

public abstract class AbstractManager<DTO extends AbstractDTO, ENTITY extends AbstractEntity> {

  // Let all managers override these methods to hydrate entities / dtos, also they can set mappings if required
  protected DTO covertToDTO(ENTITY e, DTO d) {
    d.setId(e.getId());
    return d;
  }

  protected ENTITY covertToEntity(DTO d, ENTITY e) {
    e.setId(d.getId());
    return e;
  }

  // add common methods eg: save, delete, update etc which takes dto and converts to entity and performs the operation
}
