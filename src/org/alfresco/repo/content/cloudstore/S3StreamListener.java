/*
 * Copyright (C) 2009 Alfresco Software Limited.
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * Updated by - Abhinav Kumar Mishra
 */

package org.alfresco.repo.content.cloudstore;

import java.io.File;

import org.alfresco.service.cmr.repository.ContentIOException;
import org.alfresco.service.cmr.repository.ContentStreamListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;

/**
 * The listener interface for receiving s3Stream events.
 * The class that is interested in processing a s3Stream
 * event implements this interface, and the object created
 * with that class is registered with a component using the
 * component's <code>addS3StreamListener<code> method. When
 * the s3Stream event occurs, that object's appropriate
 * method is invoked.
 *
 * @see S3StreamEvent
 * @author Updated by - Abhinav Kumar Mishra
 */
public class S3StreamListener implements ContentStreamListener {

	/** The s3. */
	private final S3Service s3Service;
	
	/** The bucket. */
	private final S3Bucket bucket;

	/** The writer. */
	private final S3ContentWriter writer;

	/** The Constant logger. */
	private static final Log LOG = LogFactory.getLog(S3StreamListener.class);

	/**
	 * Instantiates a new s3 stream listener.
	 *
	 * @param writer the writer
	 */
	public S3StreamListener(final S3ContentWriter writer) {
		this.writer = writer;
		this.s3Service = writer.getS3Service();
		this.bucket = writer.getBucket();
	}

	/* (non-Javadoc)
	 * @see org.alfresco.service.cmr.repository.ContentStreamListener#contentStreamClosed()
	 */
	public void contentStreamClosed() throws ContentIOException {
		if(LOG.isDebugEnabled()){
		  LOG.debug("S3StreamListener.contentStreamClosed(): Retrieving Temp File Stream");
		}
		try {
			final File file = writer.getTempFile();
			final long size = file.length();
			writer.setSize(size);
			final String url = writer.getNodeUrl();
			final S3Object object = new S3Object(url);
			object.setDataInputFile(file);
			object.setContentLength(size);
			object.setContentType("application/octetstream");

			try {
				s3Service.putObject(bucket, object);
			} catch (S3ServiceException s3ServExcp) {
				if(LOG.isErrorEnabled()){
					LOG.error("S3StreamListener Failed to Upload File: "+ s3ServExcp);
				}
			} finally {
				//clean data input stream
				object.closeDataInputStream();
			} 
		} catch (Exception excp) {
			if(LOG.isErrorEnabled()){
				LOG.error("S3StreamListener Failed: "+ excp);
			}
		}
	}
}
