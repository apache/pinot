package com.linkedin.thirdeye.datalayer.bao.jdbc;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.inject.persist.Transactional;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.pojo.AbstractBean;
import com.linkedin.thirdeye.datalayer.pojo.AnomalyFunctionBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;

public class AnomalyFunctionManagerImpl extends AbstractManagerImpl<AnomalyFunctionDTO>
		implements AnomalyFunctionManager {

	private static final String FIND_DISTINCT_METRIC_BY_COLLECTION = "SELECT DISTINCT(af.metric) FROM AnomalyFunctionDTO af WHERE af.collection = :collection";

	public AnomalyFunctionManagerImpl() {
		super(AnomalyFunctionDTO.class, AnomalyFunctionBean.class);
	}

	@Override
	@Transactional
	public List<AnomalyFunctionDTO> findAllByCollection(String collection) {
		// return super.findByParams(ImmutableMap.of("collection", collection));
		Predicate predicate = Predicate.EQ("collection", collection);
		List<? extends AbstractBean> list = genericPojoDao.get(predicate, beanClass);
		
		return null;
	}

	@Override
	@Transactional
	public List<String> findDistinctMetricsByCollection(String collection) {
		// return
		// getEntityManager().createQuery(FIND_DISTINCT_METRIC_BY_COLLECTION,
		// String.class)
		// .setParameter("collection", collection).getResultList();
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.linkedin.thirdeye.datalayer.bao.IAnomalyFunctionManager#
	 * findAllActiveFunctions()
	 */
	@Override
	@Transactional
	public List<AnomalyFunctionDTO> findAllActiveFunctions() {
		// return super.findByParams(ImmutableMap.of("isActive", true));
		return null;

	}

}
