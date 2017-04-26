package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.dto.EntityToEntityMappingDTO;
import com.linkedin.thirdeye.datalayer.pojo.EntityToEntityMappingBean.MappingType;

import java.util.List;

public interface EntityToEntityMappingManager extends AbstractManager<EntityToEntityMappingDTO> {
  List<EntityToEntityMappingDTO> findByFromUrn(String fromUrn);
  List<EntityToEntityMappingDTO> findByToUrn(String toUrn);
  EntityToEntityMappingDTO findByFromAndToUrn(String fromUrn, String toUrn);
  List<EntityToEntityMappingDTO> findByMappingType(MappingType mappingType);
  List<EntityToEntityMappingDTO> findByFromUrnAndMappingType(String fromUrn, MappingType mappingType);
  List<EntityToEntityMappingDTO> findByToUrnAndMappingType(String toUrn, MappingType mappingType);
}
