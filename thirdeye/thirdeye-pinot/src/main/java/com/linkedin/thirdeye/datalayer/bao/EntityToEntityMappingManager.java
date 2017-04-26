package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.dto.EntityToEntityMappingDTO;
import com.linkedin.thirdeye.datalayer.pojo.EntityToEntityMappingBean.MappingType;

import java.util.List;

public interface EntityToEntityMappingManager extends AbstractManager<EntityToEntityMappingDTO> {
  List<EntityToEntityMappingDTO> findByFromURN(String fromURN);
  List<EntityToEntityMappingDTO> findByToURN(String toURN);
  EntityToEntityMappingDTO findByFromAndToURN(String fromURN, String toURN);
  List<EntityToEntityMappingDTO> findByMappingType(MappingType mappingType);
  List<EntityToEntityMappingDTO> findByFromURNAndMappingType(String fromURN, MappingType mappingType);
  List<EntityToEntityMappingDTO> findByToURNAndMappingType(String toURN, MappingType mappingType);
}
