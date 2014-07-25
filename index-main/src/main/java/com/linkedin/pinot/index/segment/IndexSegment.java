package com.linkedin.pinot.index.segment;

import java.util.Iterator;

import com.linkedin.pinot.index.IndexType;
import com.linkedin.pinot.index.common.Predicate;
import com.linkedin.pinot.index.data.RowEvent;
import com.linkedin.pinot.index.operator.DataSource;
import com.linkedin.pinot.index.query.FilterQuery;

/**
 * This is the interface of index segment. The index type of index segment
 * should be one of the supported {@link com.linkedin.pinot.index.IndexType
 * IndexType}.
 * 
 * @author Xiang Fu <xiafu@linkedin.com>
 * 
 */
public interface IndexSegment {
	/**
	 * @return
	 */
	public IndexType getIndexType();

	/**
	 * @return
	 */
	public String getSegmentName();

	/**
	 * @return
	 */
	public String getAssociatedDirectory();


	/**
	 * @return SegmentMetadata
	 */
	public SegmentMetadata getSegmentMetadata();

	/**
	 * @param query
	 * @return Iterator<RowEvent>
	 */
	public Iterator<RowEvent> processFilterQuery(FilterQuery query);

	/**
	 * @param query
	 * @return Iterator<Integer>
	 */
	public Iterator<Integer> getDocIdIterator(FilterQuery query);

	/**
	 * @param column
	 * @return ColumnarReader
	 */
	public ColumnarReader getColumnarReader(String column);

	/**
	 * 
	 * @param columnName
	 * @return
	 */
	DataSource getDataSource(String columnName);

	/**
	 * 
	 * @param columnName
	 * @param p
	 * @return
	 */
	DataSource getDataSource(String columnName, Predicate p);
}
