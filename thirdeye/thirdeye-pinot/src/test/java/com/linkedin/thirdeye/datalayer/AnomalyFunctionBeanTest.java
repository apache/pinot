package com.linkedin.thirdeye.datalayer;

import java.util.List;

import org.junit.Assert;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.datalayer.dao.GenericPojoDao;
import com.linkedin.thirdeye.datalayer.pojo.AnomalyFunctionBean;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;
import com.linkedin.thirdeye.datalayer.util.Predicate;

public class AnomalyFunctionBeanTest extends AbstractBaseTest {

	@Test
	public void testAll() {
		GenericPojoDao genericPojoDao = DaoProviderUtil.getInstance(GenericPojoDao.class);
		AnomalyFunctionBean anomalyFunctionBean = new AnomalyFunctionBean();
		anomalyFunctionBean.setFunctionName("testFunction");
		anomalyFunctionBean.setIsActive(true);
		anomalyFunctionBean.setCollection("testCollection");
		anomalyFunctionBean.setMetric("testMetric");
		anomalyFunctionBean.setMetricId(1L);

		// TEST PUT
		Long id = genericPojoDao.put(anomalyFunctionBean);
		System.out.println("id:" + id);
		Assert.assertNotNull(id);
		Assert.assertEquals(id, (Long) 1L);

		// TEST GET PK lookup
		AnomalyFunctionBean retrivedById = genericPojoDao.get(id, AnomalyFunctionBean.class);
		Assert.assertEquals(id, retrivedById.getId());
		Assert.assertEquals("testCollection", retrivedById.getCollection());

		// TEST INDEX lookup
		Predicate predicate;
		predicate = Predicate.EQ("metricId", 1L);
		List<AnomalyFunctionBean> retrievedByPredicate = genericPojoDao.get(predicate, AnomalyFunctionBean.class);
		Assert.assertEquals(1, retrievedByPredicate.size());

		// TEST UPDATE
		retrivedById.setCollection("testCollection-new");
		int rowsUpdate = genericPojoDao.update(retrivedById);
		Assert.assertEquals(rowsUpdate, 1);
		AnomalyFunctionBean retrivedAfterUpdateById = genericPojoDao.get(id, AnomalyFunctionBean.class);
		Assert.assertEquals(id, retrivedAfterUpdateById.getId());
		Assert.assertEquals("testCollection-new", retrivedById.getCollection());

		// ensure that index is also updated

		predicate = Predicate.EQ("collection", "testCollection-new");
		List<AnomalyFunctionBean> retrievedAfterUpdateByPredicate = genericPojoDao.get(predicate,
				AnomalyFunctionBean.class);
		Assert.assertEquals(1, retrievedByPredicate.size());
		Assert.assertEquals("testCollection-new", retrievedAfterUpdateByPredicate.get(0).getCollection());

		// test delete
		int numRowsDeleted = genericPojoDao.delete(id, AnomalyFunctionBean.class);
		Assert.assertEquals(1, numRowsDeleted);
		AnomalyFunctionBean afterDelete = genericPojoDao.get(id, AnomalyFunctionBean.class);
		Assert.assertNull(afterDelete);

	}

	public static void main(String[] args) throws Exception {
		AnomalyFunctionBeanTest test = new AnomalyFunctionBeanTest();
		test.init();
		test.initDB();
		test.testAll();
		test.cleanUp();
	}
}
