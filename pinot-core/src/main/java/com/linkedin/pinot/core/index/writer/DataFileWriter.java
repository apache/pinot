package com.linkedin.pinot.core.index.writer;

import com.linkedin.pinot.core.index.reader.DataFileMetadata;

/**
 * 
 * @author kgopalak
 * 
 */
public interface DataFileWriter {
	/**
	 * 
	 * @return
	 */
	boolean open();

	/**
	 * 
	 * @param metadata
	 * @return
	 */
	boolean setMetadata(DataFileMetadata metadata);

	/**
	 * 
	 * @return
	 */
	boolean close();

}
