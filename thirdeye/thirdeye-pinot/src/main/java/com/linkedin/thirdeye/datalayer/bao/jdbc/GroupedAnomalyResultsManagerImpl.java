package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.google.inject.Singleton;
import com.linkedin.thirdeye.datalayer.bao.GroupedAnomalyResultsManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.GroupedAnomalyResultsDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.pojo.GroupedAnomalyResultsBean;
import com.linkedin.thirdeye.datalayer.pojo.MergedAnomalyResultBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;
import com.linkedin.thirdeye.datasource.DAORegistry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.modelmapper.ModelMapper;

@Singleton
public class GroupedAnomalyResultsManagerImpl extends AbstractManagerImpl<GroupedAnomalyResultsDTO>
    implements GroupedAnomalyResultsManager {

  protected static final ModelMapper MODEL_MAPPER = new ModelMapper();

  protected GroupedAnomalyResultsManagerImpl() {
    super(GroupedAnomalyResultsDTO.class, GroupedAnomalyResultsBean.class);
  }

  @Override
  public Long save(GroupedAnomalyResultsDTO groupedAnomalyResultDTO) {
    if (groupedAnomalyResultDTO.getId() != null) {
      update(groupedAnomalyResultDTO);
      return groupedAnomalyResultDTO.getId();
    } else {
      GroupedAnomalyResultsBean bean = convertGroupedAnomalyDTO2Bean(groupedAnomalyResultDTO);
      Long id = genericPojoDao.put(bean);
      groupedAnomalyResultDTO.setId(id);
      return id;
    }
  }

  @Override
  public int update(GroupedAnomalyResultsDTO groupedAnomalyResultDTO) {
    if (groupedAnomalyResultDTO.getId() == null) {
      Long id = save(groupedAnomalyResultDTO);
      if (id > 0) {
        return 1;
      } else {
        return 0;
      }
    } else {
      GroupedAnomalyResultsBean groupedAnomalyResultsBean = convertGroupedAnomalyDTO2Bean(groupedAnomalyResultDTO);
      return genericPojoDao.update(groupedAnomalyResultsBean);
    }
  }

  @Override
  public GroupedAnomalyResultsDTO findById(Long id) {
    GroupedAnomalyResultsBean bean = genericPojoDao.get(id, GroupedAnomalyResultsBean.class);
    if (bean != null) {
      GroupedAnomalyResultsDTO groupedAnomalyResultsDTO = convertGroupedAnomalyBean2DTO(bean);
      return groupedAnomalyResultsDTO;
    } else {
      return null;
    }
  }

  @Override
  public GroupedAnomalyResultsDTO findMostRecentInTimeWindow(long alertConfigId, String dimensions, long windowStart,
      long windowEnd) {
    Predicate predicate = Predicate
        .AND(Predicate.EQ("alertConfigId", alertConfigId), Predicate.EQ("dimensions", dimensions),
            Predicate.GT("endTime", windowStart), Predicate.LE("endTime", windowEnd));

    List<GroupedAnomalyResultsBean> groupedAnomalyResultsBeans =
        genericPojoDao.get(predicate, GroupedAnomalyResultsBean.class);
    if (CollectionUtils.isNotEmpty(groupedAnomalyResultsBeans)) {
      // Sort grouped anomaly results bean in the natural order of their end time.
      Collections.sort(groupedAnomalyResultsBeans, new Comparator<GroupedAnomalyResultsBean>() {
        @Override
        public int compare(GroupedAnomalyResultsBean o1, GroupedAnomalyResultsBean o2) {
          int endTimeCompare = (int) (o1.getEndTime() - o2.getEndTime());
          if (endTimeCompare != 0) {
            return endTimeCompare;
          } else {
            return (int) (o1.getId() - o2.getId());
          }
        }
      });
      GroupedAnomalyResultsDTO groupedAnomalyResultsDTO =
          convertGroupedAnomalyBean2DTO(groupedAnomalyResultsBeans.get(groupedAnomalyResultsBeans.size() - 1));
      return groupedAnomalyResultsDTO;
    } else {
      return null;
    }
  }

  protected GroupedAnomalyResultsBean convertGroupedAnomalyDTO2Bean(GroupedAnomalyResultsDTO entity) {
    GroupedAnomalyResultsBean bean = convertDTO2Bean(entity, GroupedAnomalyResultsBean.class);
    if (CollectionUtils.isNotEmpty(entity.getAnomalyResults())) {
      List<Long> mergedAnomalyId = new ArrayList<>();
      for (MergedAnomalyResultDTO mergedAnomalyResultDTO : entity.getAnomalyResults()) {
        mergedAnomalyId.add(mergedAnomalyResultDTO.getId());
      }
      bean.setAnomalyResultsId(mergedAnomalyId);
    }
    return bean;
  }

  /**
   * Convert grouped anomaly bean to DTO. The merged anomaly results in this group are also converted to their
   * corresponding DTO class; however, the raw anomalies of those merged results are not converted.
   *
   * @param groupedAnomalyResultsBean the bean class to be converted
   *
   * @return the DTO class that consists of the DTO of merged anomalies whose raw anomalies are not converted from bean.
   */
  protected GroupedAnomalyResultsDTO convertGroupedAnomalyBean2DTO(
      GroupedAnomalyResultsBean groupedAnomalyResultsBean) {
    GroupedAnomalyResultsDTO groupedAnomalyResultsDTO =
        MODEL_MAPPER.map(groupedAnomalyResultsBean, GroupedAnomalyResultsDTO.class);

    if (CollectionUtils.isNotEmpty(groupedAnomalyResultsBean.getAnomalyResultsId())) {
      List<MergedAnomalyResultBean> list =
          genericPojoDao.get(groupedAnomalyResultsBean.getAnomalyResultsId(), MergedAnomalyResultBean.class);
      MergedAnomalyResultManager mergedAnomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
      List<MergedAnomalyResultDTO> mergedAnomalyResults =
          mergedAnomalyDAO.convertMergedAnomalyBean2DTO(list, true);
      groupedAnomalyResultsDTO.setAnomalyResults(mergedAnomalyResults);
    }

    return groupedAnomalyResultsDTO;
  }
}
