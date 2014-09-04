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

import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import org.alfresco.repo.content.AbstractContentReader;
import org.alfresco.service.cmr.repository.ContentIOException;
import org.alfresco.service.cmr.repository.ContentReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;

/**
 * Provides READ services against an S3 content store.
 *
 * @author Luis Sala
 * @author Updated by - Abhinav Kumar Mishra
 */
public class S3ContentReader extends AbstractContentReader {

	/**
	 * Message key for missing content. Parameters are
	 * <ul>
	 * <li>{@link org.alfresco.service.cmr.repository.NodeRef NodeRef}</li>
	 * <li>{@link ContentReader ContentReader}</li>
	 * </ul>
	 */
	public static final String MSG_MISSING_CONTENT = "content.content_missing";

	/** The Constant logger. */
	private static final Log LOG = LogFactory.getLog(S3ContentReader.class);

	/** The object details. */
	private S3Object objectDetails;

	/** The node url. */
	private String nodeUrl;

	/** The s3Service. */
	private S3Service s3Service;

	/** The bucket. */
	private S3Bucket bucket;

	/**
	 * Constructor that builds a URL based on the absolute path of the file.
	 *
	 * @param nodeUrl url of the content node.
	 * @param s3Sevice the s3 service
	 * @param bucket the bucket
	 */
	public S3ContentReader(final String nodeUrl, final S3Service s3Sevice,
			final S3Bucket bucket) {
		super(nodeUrl);
		this.nodeUrl = nodeUrl;
		this.s3Service = s3Sevice;
		this.bucket = bucket;
		getDetails();
	}

	/* (non-Javadoc)
	 * @see org.alfresco.repo.content.AbstractContentReader#createReader()
	 */
	@Override
	protected ContentReader createReader() throws ContentIOException {
		if(LOG.isDebugEnabled()){
			LOG.debug("S3ContentReader.createReader() invoked for contentUrl="+nodeUrl);
		}
		return new S3ContentReader(nodeUrl, s3Service, bucket);
	}

	/* (non-Javadoc)
	 * @see org.alfresco.repo.content.AbstractContentReader#getDirectReadableChannel()
	 */
	@Override
	protected ReadableByteChannel getDirectReadableChannel() throws ContentIOException {
		try {
			// Confirm the requested object exists
			if (!exists()) {
				throw new ContentIOException("Content object does not exist");
			}
			if(LOG.isDebugEnabled()){
				LOG.debug("S3ContentReader Obtaining Input Stream: nodeUrl="+nodeUrl);
			}
			// Get the object and retrieve the input stream
			final S3Object object = s3Service.getObject(bucket, nodeUrl);
			ReadableByteChannel channel = null;
			final InputStream is = object.getDataInputStream();
			channel = Channels.newChannel(is);
			if(LOG.isDebugEnabled()){
				LOG.debug("S3ContentReader Success Obtaining Input Stream: nodeUrl="+nodeUrl);
			}
			return channel;
		} catch (Exception excp) {
			throw new ContentIOException("Failed to open channel: " + this, excp);
		}
	} 

	/* (non-Javadoc)
	 * @see org.alfresco.service.cmr.repository.ContentReader#exists()
	 */
	public boolean exists() {
		return (objectDetails != null);
	}  

	/* (non-Javadoc)
	 * @see org.alfresco.service.cmr.repository.ContentReader#getLastModified()
	 */
	public long getLastModified() {
		if (objectDetails == null) {
			return 0L;
		}
		return objectDetails.getLastModifiedDate().getTime();
	} 
	
	/* (non-Javadoc)
	 * @see org.alfresco.service.cmr.repository.ContentAccessor#getSize()
	 */
	public long getSize() {
		if (!exists()) {
			return 0L;
		}
		try {
			return objectDetails.getContentLength();
		} catch (Exception excp) {
			if(LOG.isErrorEnabled()){
				LOG.error("S3ContentReader Failed to getContentLength, Returning '0L' size as default: "
						+ excp.getMessage());
			}
			return 0L;
		}
	}


	/**
	 * Gets information on a stream. Returns headers from the response.
	 *
	 * @return Header[] array of http headers from the response
	 */
	private void getDetails() {
		if (objectDetails != null) {
			// Info already fetched, so don't do this again.
			return;
		}
		try {
			objectDetails = s3Service.getObjectDetails(bucket, nodeUrl);
		} catch (S3ServiceException s3ServExcp) {
			if(LOG.isErrorEnabled()){
				LOG.error("S3ContentReader Failed to get Object Details: " + s3ServExcp);
			}
		} finally {
			cleanup();
		}
	} 
	
	/**
	 * Cleanup.
	 */
	private void cleanup() {
		// TODO Perform any cleanup operations
	} 
} 
