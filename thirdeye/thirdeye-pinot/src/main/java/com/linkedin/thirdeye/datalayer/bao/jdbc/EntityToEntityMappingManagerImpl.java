package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.linkedin.thirdeye.datalayer.bao.EntityToEntityMappingManager;
import com.linkedin.thirdeye.datalayer.dto.EntityToEntityMappingDTO;
import com.linkedin.thirdeye.datalayer.pojo.EntityToEntityMappingBean;
import com.linkedin.thirdeye.datalayer.pojo.EntityToEntityMappingBean.MappingType;
import com.linkedin.thirdeye.datalayer.util.Predicate;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;

public class EntityToEntityMappingManagerImpl extends AbstractManagerImpl<EntityToEntityMappingDTO> implements EntityToEntityMappingManager {
  protected EntityToEntityMappingManagerImpl() {
    super(EntityToEntityMappingDTO.class, EntityToEntityMappingBean.class);
  }

  @Override
  public List<EntityToEntityMappingDTO> findByFromUrn(String fromUrn) {
    Predicate predicate = Predicate.EQ("fromUrn", fromUrn);
    return findByPredicate(predicate);
  }

  @Override
  public List<EntityToEntityMappingDTO> findByToUrn(String toUrn) {
    Predicate predicate = Predicate.EQ("toUrn", toUrn);
    return findByPredicate(predicate);
  }

  @Override
  public EntityToEntityMappingDTO findByFromAndToUrn(String fromUrn, String toUrn) {
    EntityToEntityMappingDTO dto = null;
    Predicate predicate = Predicate.AND(Predicate.EQ("fromUrn", fromUrn), Predicate.EQ("toUrn", toUrn));
    List<EntityToEntityMappingDTO> findByPredicate = findByPredicate(predicate);
    if (CollectionUtils.isNotEmpty(findByPredicate)) {
      dto = findByPredicate.get(0);
    }
    return dto;
  }

  @Override
  public List<EntityToEntityMappingDTO> findByMappingType(MappingType mappingType) {
    Predicate predicate = Predicate.EQ("mappingType", mappingType.toString());
    return findByPredicate(predicate);
  }

  @Override
  public List<EntityToEntityMappingDTO> findByFromUrnAndMappingType(String fromUrn, MappingType mappingType) {
    Predicate predicate = Predicate.AND(Predicate.EQ("fromUrn", fromUrn), Predicate.EQ("mappingType", mappingType.toString()));
    return findByPredicate(predicate);
  }

  @Override
  public List<EntityToEntityMappingDTO> findByToUrnAndMappingType(String toUrn, MappingType mappingType) {
    Predicate predicate = Predicate.AND(Predicate.EQ("toUrn", toUrn), Predicate.EQ("mappingType", mappingType.toString()));
    return findByPredicate(predicate);
  }
}
