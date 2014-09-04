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
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import org.alfresco.repo.content.AbstractContentWriter;
import org.alfresco.service.cmr.repository.ContentData;
import org.alfresco.service.cmr.repository.ContentIOException;
import org.alfresco.service.cmr.repository.ContentReader;
import org.alfresco.util.GUID;
import org.alfresco.util.TempFileProvider;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jets3t.service.S3Service;
import org.jets3t.service.model.S3Bucket;

/**
 * The Class S3ContentWriter.
 * 
 * @author Updated by - Abhinav Kumar Mishra
 */
public class S3ContentWriter extends AbstractContentWriter {

	/** The node url. */
	private String nodeUrl;

	/** The uuid. */
	private String uuid;

	/** The temp file. */
	private File tempFile;

	/** The size. */
	private long size;

	/** The s3Service. */
	private final S3Service s3Service;

	/** The bucket. */
	private final S3Bucket bucket;

	/** The Constant logger. */
	private static final Log LOG = LogFactory.getLog(S3ContentWriter.class);

	/**
	 * Instantiates a new s3 content writer.
	 *
	 * @param nodeUrl the node url
	 * @param existingContentReader the existing content reader
	 * @param s3Service the s3
	 * @param bucket the bucket
	 */
	public S3ContentWriter(final String nodeUrl, final ContentReader existingContentReader,
			final S3Service s3Service, final S3Bucket bucket) {
		super(nodeUrl, existingContentReader);
		this.nodeUrl = nodeUrl;
		this.s3Service=s3Service;
		this.bucket=bucket;
		this.uuid=GUID.generate();
		addListener(new S3StreamListener(this));
	}

	/* (non-Javadoc)
	 * @see org.alfresco.repo.content.AbstractContentWriter#createReader()
	 */
	@Override
	protected ContentReader createReader() throws ContentIOException {
		return new S3ContentReader(getContentUrl(), s3Service, bucket);
	}

	/* (non-Javadoc)
	 * @see org.alfresco.repo.content.AbstractContentWriter#getDirectWritableChannel()
	 */
	@Override
	protected WritableByteChannel getDirectWritableChannel()
			throws ContentIOException {
		try {
			if(LOG.isDebugEnabled()){
				LOG.debug("S3ContentWriter Creating Temp File: uuid=" + uuid);
			}
			tempFile = TempFileProvider.createTempFile(uuid, ".bin");
			final OutputStream outStream = new FileOutputStream(tempFile);
			if(LOG.isDebugEnabled()){
				LOG.debug("S3ContentWriter Returning Channel to Temp File: uuid="+ uuid);
			}
			return Channels.newChannel(outStream);
		} catch (Exception excp) {
			throw new ContentIOException(
					"S3ContentWriter.getDirectWritableChannel(): Failed to open channel. "
							+ this, excp);
		}
	}

	/* (non-Javadoc)
	 * @see org.alfresco.repo.content.AbstractContentAccessor#getContentData()
	 */
	@Override
	public ContentData getContentData() {
		return new ContentData(getNodeUrl(), getMimetype(), getSize(),
				getEncoding());
	}

	/**
	 * Gets the node url.
	 *
	 * @return the node url
	 */
	public String getNodeUrl() {
		return nodeUrl;
	}

	/**
	 * Sets the node url.
	 *
	 * @param nodeUrl the new node url
	 */
	public void setNodeUrl(final String nodeUrl) {
		this.nodeUrl = nodeUrl;
	}

	/**
	 * Gets the uuid.
	 *
	 * @return the uuid
	 */
	public String getUuid() {
		return uuid;
	}

	/**
	 * Sets the uuid.
	 *
	 * @param uuid the new uuid
	 */
	public void setUuid(final String uuid) {
		this.uuid = uuid;
	}


	/**
	 * Gets the temp file.
	 *
	 * @return the temp file
	 */
	public File getTempFile() {
		return tempFile;
	}

	/* (non-Javadoc)
	 * @see org.alfresco.service.cmr.repository.ContentAccessor#getSize()
	 */
	public long getSize() {
		return size;
	}

	/**
	 * Sets the size.
	 *
	 * @param size the new size
	 */
	public void setSize(final long size) {
		this.size = size;
	}

	/**
	 * Gets the s3 service.
	 *
	 * @return the s3 service
	 */
	public S3Service getS3Service() {
		return s3Service;
	}

	/**
	 * Gets the bucket.
	 *
	 * @return the bucket
	 */
	public S3Bucket getBucket() {
		return bucket;
	}
}