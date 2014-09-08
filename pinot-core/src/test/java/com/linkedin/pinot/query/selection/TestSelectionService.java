package com.linkedin.pinot.query.selection;

import static org.testng.AssertJUnit.assertEquals;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.query.ReduceService;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.Selection;
import com.linkedin.pinot.common.request.SelectionSort;
import com.linkedin.pinot.common.response.BrokerResponse;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.query.reduce.DefaultReduceService;
import com.linkedin.pinot.core.query.selection.SelectionService;
import com.linkedin.pinot.core.query.utils.IndexSegmentUtils;


public class TestSelectionService {

  private static BrokerRequest _brokerRequest;
  private static IndexSegment _indexSegment;
  private static IndexSegment _indexSegment2;
  private static IndexSegment _indexSegmentWithSchema1;
  private static IndexSegment _indexSegmentWithSchema2;
  private static int _indexSize = 20000001;

  @BeforeClass
  public static void setup() {
    _brokerRequest = getSelectionNoFilterBrokerRequest();
    _indexSegment = IndexSegmentUtils.getIndexSegmentWithAscendingOrderValues(_indexSize);
    _indexSegment2 = IndexSegmentUtils.getIndexSegmentWithAscendingOrderValues(_indexSize);
    Schema schema1 = new Schema();
    schema1.addSchema("dim0", new FieldSpec("dim0", FieldType.dimension, DataType.DOUBLE, true));
    schema1.addSchema("dim1", new FieldSpec("dim1", FieldType.dimension, DataType.DOUBLE, true));
    schema1.addSchema("met", new FieldSpec("met", FieldType.metric, DataType.DOUBLE, true));
    Schema schema2 = new Schema();
    schema2.addSchema("dim0", new FieldSpec("dim0", FieldType.dimension, DataType.DOUBLE, true));
    schema2.addSchema("dim1", new FieldSpec("dim1", FieldType.dimension, DataType.DOUBLE, true));
    schema2.addSchema("met", new FieldSpec("met", FieldType.metric, DataType.DOUBLE, true));

    _indexSegmentWithSchema1 = IndexSegmentUtils.getIndexSegmentWithAscendingOrderValues(20000001, schema1);
    _indexSegmentWithSchema2 = IndexSegmentUtils.getIndexSegmentWithAscendingOrderValues(20000001, schema2);

  }

  @Test
  public void testEmptySelectionService() {
    BrokerRequest brokerRequest = _brokerRequest.deepCopy();
    SelectionService selectionService = new SelectionService(brokerRequest.getSelections(), _indexSegment);
    Assert.assertEquals(selectionService.getNumDocsScanned(), 0);
    PriorityQueue<Serializable[]> rowEventsSet = selectionService.getRowEventsSet();
    assertEquals(true, rowEventsSet.isEmpty());
  }

  @Test
  public void testMapDocSelectionService() {
    BrokerRequest brokerRequest = _brokerRequest.deepCopy();
    brokerRequest.getSelections().getSelectionSortSequence().get(0).setIsAsc(false);
    SelectionService selectionService = new SelectionService(brokerRequest.getSelections(), _indexSegment);
    for (int i = 0; i < _indexSize; ++i) {
      selectionService.mapDoc(i);
    }
    Assert.assertEquals(selectionService.getNumDocsScanned(), _indexSize);
    PriorityQueue<Serializable[]> rowEventsSet = selectionService.getRowEventsSet();

    System.out.println(selectionService.getDataSchema().toString());
    while (!rowEventsSet.isEmpty()) {
      Serializable[] rowSerializables = rowEventsSet.poll();
      Assert.assertEquals(((Double) rowSerializables[0]).doubleValue(), 9.0);
      Assert.assertEquals(((Double) rowSerializables[1]).doubleValue(), 99.0);
      System.out.println(Arrays.toString(rowSerializables));
    }
  }

  @Test
  public void testMergeInSelectionService() {
    BrokerRequest brokerRequest = _brokerRequest.deepCopy();
    SelectionService selectionService1 = new SelectionService(brokerRequest.getSelections(), _indexSegment);
    for (int i = 0; i < 40; ++i) {
      selectionService1.mapDoc(i);
    }
    PriorityQueue<Serializable[]> rowEventsSet1 = selectionService1.getRowEventsSet();

    SelectionService selectionService2 = new SelectionService(brokerRequest.getSelections(), _indexSegment2);
    for (int i = 0; i < 40; ++i) {
      selectionService2.mapDoc(i);
    }
    PriorityQueue<Serializable[]> rowEventsSet2 = selectionService2.getRowEventsSet();

    PriorityQueue<Serializable[]> rowEventsSet = selectionService1.merge(rowEventsSet1, rowEventsSet2);
    System.out.println(selectionService1.getDataSchema().toString());
    while (!rowEventsSet.isEmpty()) {
      Serializable[] rowSerializables = rowEventsSet.poll();
      System.out.println(Arrays.toString(rowSerializables));
    }
  }

  @Test
  public void testToDataTable() throws Exception {
    BrokerRequest brokerRequest = _brokerRequest.deepCopy();
    SelectionService selectionService1 = new SelectionService(brokerRequest.getSelections(), _indexSegment);
    for (int i = 0; i < 40; ++i) {
      selectionService1.mapDoc(i);
    }
    PriorityQueue<Serializable[]> rowEventsSet1 = selectionService1.getRowEventsSet();
    SelectionService selectionService2 = new SelectionService(brokerRequest.getSelections(), _indexSegment2);
    for (int i = 40; i < 80; ++i) {
      selectionService2.mapDoc(i);
    }
    PriorityQueue<Serializable[]> rowEventsSet2 = selectionService2.getRowEventsSet();
    DataTable dataTable1 =
        SelectionService.transformRowSetToDataTable(rowEventsSet1, selectionService1.getDataSchema());
    DataTable dataTable2 =
        SelectionService.transformRowSetToDataTable(rowEventsSet2, selectionService1.getDataSchema());
    dataTable1.getMetadata().put("numDocsScanned", 40 + "");
    dataTable1.getMetadata().put("totalDocs", 80 + "");
    dataTable1.getMetadata().put("timeUsedMs", 120 + "");
    dataTable2.getMetadata().put("numDocsScanned", 40 + "");
    dataTable2.getMetadata().put("totalDocs", 240 + "");
    dataTable2.getMetadata().put("timeUsedMs", 180 + "");

    Map<ServerInstance, DataTable> instanceToDataTableMap = new HashMap<ServerInstance, DataTable>();
    instanceToDataTableMap.put(new ServerInstance("localhost:0000"), dataTable1);
    instanceToDataTableMap.put(new ServerInstance("localhost:1111"), dataTable2);
    ReduceService reduceService = new DefaultReduceService();
    BrokerResponse brokerResponse = reduceService.reduceOnDataTable(brokerRequest, instanceToDataTableMap);
    Assert.assertEquals(brokerResponse.getSelectionResults().getJSONArray("results").length(), brokerRequest
        .getSelections().getSize());
    System.out.println(brokerResponse);

  }

  @Test
  public void testRender() throws Exception {
    BrokerRequest brokerRequest = _brokerRequest.deepCopy();
    SelectionService selectionService1 = new SelectionService(brokerRequest.getSelections(), _indexSegment);
    for (int i = 0; i < 40; ++i) {
      selectionService1.mapDoc(i);
    }
    PriorityQueue<Serializable[]> rowEventsSet1 = selectionService1.getRowEventsSet();
    SelectionService selectionService2 = new SelectionService(brokerRequest.getSelections(), _indexSegment2);
    for (int i = 40; i < 80; ++i) {
      selectionService2.mapDoc(i);
    }
    PriorityQueue<Serializable[]> rowEventsSet2 = selectionService2.getRowEventsSet();
    DataTable dataTable1 =
        SelectionService.transformRowSetToDataTable(rowEventsSet1, selectionService1.getDataSchema());
    DataTable dataTable2 =
        SelectionService.transformRowSetToDataTable(rowEventsSet2, selectionService1.getDataSchema());
    dataTable1.getMetadata().put("numDocsScanned", 40 + "");
    dataTable1.getMetadata().put("totalDocs", 80 + "");
    dataTable1.getMetadata().put("timeUsedMs", 120 + "");
    dataTable2.getMetadata().put("numDocsScanned", 40 + "");
    dataTable2.getMetadata().put("totalDocs", 240 + "");
    dataTable2.getMetadata().put("timeUsedMs", 180 + "");

    Map<ServerInstance, DataTable> instanceToDataTableMap = new HashMap<ServerInstance, DataTable>();
    instanceToDataTableMap.put(new ServerInstance("localhost:0000"), dataTable1);
    instanceToDataTableMap.put(new ServerInstance("localhost:1111"), dataTable2);
    ReduceService reduceService = new DefaultReduceService();
    BrokerResponse brokerResponse = reduceService.reduceOnDataTable(brokerRequest, instanceToDataTableMap);
    Assert.assertEquals(brokerResponse.getSelectionResults().getJSONArray("results").length(), brokerRequest
        .getSelections().getSize());
    System.out.println(brokerResponse.getSelectionResults());

  }

  @Test
  public void testSelectionStar() throws Exception {
    BrokerRequest brokerRequest = _brokerRequest.deepCopy();
    List<String> selectionColumns = brokerRequest.getSelections().getSelectionColumns();
    selectionColumns.clear();
    selectionColumns.add("*");
    SelectionService selectionService1 = new SelectionService(brokerRequest.getSelections(), _indexSegmentWithSchema1);
    for (int i = 0; i < 40; ++i) {
      selectionService1.mapDoc(i);
    }
    PriorityQueue<Serializable[]> rowEventsSet1 = selectionService1.getRowEventsSet();
    SelectionService selectionService2 = new SelectionService(brokerRequest.getSelections(), _indexSegmentWithSchema2);
    for (int i = 40; i < 80; ++i) {
      selectionService2.mapDoc(i);
    }
    PriorityQueue<Serializable[]> rowEventsSet2 = selectionService2.getRowEventsSet();
    DataTable dataTable1 =
        SelectionService.transformRowSetToDataTable(rowEventsSet1, selectionService1.getDataSchema());
    DataTable dataTable2 =
        SelectionService.transformRowSetToDataTable(rowEventsSet2, selectionService1.getDataSchema());
    dataTable1.getMetadata().put("numDocsScanned", 40 + "");
    dataTable1.getMetadata().put("totalDocs", 80 + "");
    dataTable1.getMetadata().put("timeUsedMs", 120 + "");
    dataTable2.getMetadata().put("numDocsScanned", 40 + "");
    dataTable2.getMetadata().put("totalDocs", 240 + "");
    dataTable2.getMetadata().put("timeUsedMs", 180 + "");

    Map<ServerInstance, DataTable> instanceToDataTableMap = new HashMap<ServerInstance, DataTable>();
    instanceToDataTableMap.put(new ServerInstance("localhost:0000"), dataTable1);
    instanceToDataTableMap.put(new ServerInstance("localhost:1111"), dataTable2);
    ReduceService reduceService = new DefaultReduceService();
    BrokerResponse brokerResponse = reduceService.reduceOnDataTable(brokerRequest, instanceToDataTableMap);
    Assert.assertEquals(brokerResponse.getSelectionResults().getJSONArray("results").length(), brokerRequest
        .getSelections().getSize());
    System.out.println(brokerResponse.getSelectionResults());
  }

  @Test
  public void testSelectionStarWithoutOrdering() throws Exception {
    BrokerRequest brokerRequest = _brokerRequest.deepCopy();
    List<String> selectionColumns = brokerRequest.getSelections().getSelectionColumns();
    brokerRequest.getSelections().setSelectionSortSequence(null);
    selectionColumns.clear();
    selectionColumns.add("*");
    SelectionService selectionService1 = new SelectionService(brokerRequest.getSelections(), _indexSegmentWithSchema1);
    for (int i = 0; i < 40; ++i) {
      selectionService1.mapDoc(i);
    }
    PriorityQueue<Serializable[]> rowEventsSet1 = selectionService1.getRowEventsSet();
    SelectionService selectionService2 = new SelectionService(brokerRequest.getSelections(), _indexSegmentWithSchema2);
    for (int i = 40; i < 80; ++i) {
      selectionService2.mapDoc(i);
    }
    PriorityQueue<Serializable[]> rowEventsSet2 = selectionService2.getRowEventsSet();
    DataTable dataTable1 =
        SelectionService.transformRowSetToDataTable(rowEventsSet1, selectionService1.getDataSchema());
    DataTable dataTable2 =
        SelectionService.transformRowSetToDataTable(rowEventsSet2, selectionService1.getDataSchema());
    dataTable1.getMetadata().put("numDocsScanned", 40 + "");
    dataTable1.getMetadata().put("totalDocs", 80 + "");
    dataTable1.getMetadata().put("timeUsedMs", 120 + "");
    dataTable2.getMetadata().put("numDocsScanned", 40 + "");
    dataTable2.getMetadata().put("totalDocs", 240 + "");
    dataTable2.getMetadata().put("timeUsedMs", 180 + "");

    Map<ServerInstance, DataTable> instanceToDataTableMap = new HashMap<ServerInstance, DataTable>();
    instanceToDataTableMap.put(new ServerInstance("localhost:0000"), dataTable1);
    instanceToDataTableMap.put(new ServerInstance("localhost:1111"), dataTable2);
    ReduceService reduceService = new DefaultReduceService();
    BrokerResponse brokerResponse = reduceService.reduceOnDataTable(brokerRequest, instanceToDataTableMap);
    Assert.assertEquals(brokerResponse.getSelectionResults().getJSONArray("results").length(), brokerRequest
        .getSelections().getSize());
    System.out.println(brokerResponse.getSelectionResults());
  }

  private static BrokerRequest getSelectionNoFilterBrokerRequest() {
    BrokerRequest brokerRequest = new BrokerRequest();
    brokerRequest.setSelections(getSelectionQuery());
    return brokerRequest;
  }

  private static Selection getSelectionQuery() {
    Selection selection = new Selection();
    selection.setOffset(0);
    selection.setSize(80);
    List<String> selectionColumns = new ArrayList<String>();
    selectionColumns.add("dim0");
    selectionColumns.add("dim1");
    selectionColumns.add("met");
    selection.setSelectionColumns(selectionColumns);

    List<SelectionSort> selectionSortSequence = new ArrayList<SelectionSort>();
    SelectionSort selectionSort = new SelectionSort();
    selectionSort.setColumn("dim0");
    selectionSort.setIsAsc(true);
    selectionSortSequence.add(selectionSort);
    selectionSort = new SelectionSort();
    selectionSort.setColumn("dim1");
    selectionSort.setIsAsc(false);
    selectionSortSequence.add(selectionSort);

    selection.setSelectionSortSequence(selectionSortSequence);

    return selection;
  }

  @Test
  public void testMapDocSelectionServiceNoOrdering() {
    BrokerRequest brokerRequest = _brokerRequest.deepCopy();
    brokerRequest.getSelections().setSelectionSortSequence(null);
    SelectionService selectionService = new SelectionService(brokerRequest.getSelections(), _indexSegment);
    for (int i = 0; i < _indexSize; ++i) {
      selectionService.mapDoc(i);
    }
    Assert.assertEquals(selectionService.getNumDocsScanned(), _indexSize);
    PriorityQueue<Serializable[]> rowEventsSet = selectionService.getRowEventsSet();
    System.out.println(selectionService.getDataSchema().toString());
    while (!rowEventsSet.isEmpty()) {
      System.out.println(Arrays.toString(rowEventsSet.poll()));
    }
  }

  @Test
  public void testMergeInSelectionServiceNoOrdering() {
    BrokerRequest brokerRequest = _brokerRequest.deepCopy();
    brokerRequest.getSelections().setSelectionSortSequence(null);
    SelectionService selectionService1 = new SelectionService(brokerRequest.getSelections(), _indexSegment);
    for (int i = 0; i < 40; ++i) {
      selectionService1.mapDoc(i);
    }
    PriorityQueue<Serializable[]> rowEventsSet1 = selectionService1.getRowEventsSet();

    SelectionService selectionService2 = new SelectionService(brokerRequest.getSelections(), _indexSegment2);
    for (int i = 0; i < 40; ++i) {
      selectionService2.mapDoc(i);
    }
    PriorityQueue<Serializable[]> rowEventsSet2 = selectionService2.getRowEventsSet();

    PriorityQueue<Serializable[]> rowEventsSet = selectionService1.merge(rowEventsSet1, rowEventsSet2);
    System.out.println(selectionService1.getDataSchema().toString());
    int i = 39;
    while (!rowEventsSet.isEmpty()) {
      Serializable[] rowSerializables = rowEventsSet.poll();
      Assert.assertEquals(((Integer) rowSerializables[1]).intValue(), i--);
      if (i < 0) {
        i += 40;
      }
      System.out.println(Arrays.toString(rowSerializables));
    }
  }

  @Test
  public void testToDataTableNoOrdering() throws Exception {
    BrokerRequest brokerRequest = _brokerRequest.deepCopy();
    brokerRequest.getSelections().setSelectionSortSequence(null);
    brokerRequest.getSelections().setOffset(0);
    brokerRequest.getSelections().setSize(80);
    SelectionService selectionService1 = new SelectionService(brokerRequest.getSelections(), _indexSegment);
    for (int i = 0; i < 50; ++i) {
      selectionService1.mapDoc(i);
    }
    PriorityQueue<Serializable[]> rowEventsSet1 = selectionService1.getRowEventsSet();
    SelectionService selectionService2 = new SelectionService(brokerRequest.getSelections(), _indexSegment2);
    for (int i = 0; i < 30; ++i) {
      selectionService2.mapDoc(i);
    }
    PriorityQueue<Serializable[]> rowEventsSet2 = selectionService2.getRowEventsSet();
    DataTable dataTable1 =
        SelectionService.transformRowSetToDataTable(rowEventsSet1, selectionService1.getDataSchema());
    DataTable dataTable2 =
        SelectionService.transformRowSetToDataTable(rowEventsSet2, selectionService1.getDataSchema());
    dataTable1.getMetadata().put("numDocsScanned", 40 + "");
    dataTable1.getMetadata().put("totalDocs", 80 + "");
    dataTable1.getMetadata().put("timeUsedMs", 120 + "");
    dataTable2.getMetadata().put("numDocsScanned", 40 + "");
    dataTable2.getMetadata().put("totalDocs", 240 + "");
    dataTable2.getMetadata().put("timeUsedMs", 180 + "");

    Map<ServerInstance, DataTable> instanceToDataTableMap = new HashMap<ServerInstance, DataTable>();
    instanceToDataTableMap.put(new ServerInstance("localhost:0000"), dataTable1);
    instanceToDataTableMap.put(new ServerInstance("localhost:1111"), dataTable2);
    ReduceService reduceService = new DefaultReduceService();
    BrokerResponse brokerResponse = reduceService.reduceOnDataTable(brokerRequest, instanceToDataTableMap);
    System.out.println(brokerResponse);
    Assert.assertEquals(brokerResponse.getSelectionResults().getJSONArray("results").length(), brokerRequest
        .getSelections().getSize());
    for (int i = 0; i < 80; ++i) {
      if (i < 50) {
        Assert
            .assertEquals(
                Integer.parseInt(brokerResponse.getSelectionResults().getJSONArray("results").getJSONArray(i)
                    .getString(1)), i);
      } else {
        Assert
            .assertEquals(
                Integer.parseInt(brokerResponse.getSelectionResults().getJSONArray("results").getJSONArray(i)
                    .getString(1)), i - 50);
      }
      Assert.assertEquals(
          Integer.parseInt(brokerResponse.getSelectionResults().getJSONArray("results").getJSONArray(i).getString(0)),
          i % 10);
    }
  }
}
