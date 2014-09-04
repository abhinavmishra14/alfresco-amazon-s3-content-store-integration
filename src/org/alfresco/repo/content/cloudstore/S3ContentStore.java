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

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Properties;

import org.alfresco.repo.content.AbstractContentStore;
import org.alfresco.repo.content.ContentStore;
import org.alfresco.repo.content.filestore.FileContentStore;
import org.alfresco.service.cmr.repository.ContentIOException;
import org.alfresco.service.cmr.repository.ContentReader;
import org.alfresco.service.cmr.repository.ContentWriter;
import org.alfresco.util.GUID;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.security.AWSCredentials;

import com.abhinav.alfresco.publishing.cloudstore.CloudStoreConstants;
import com.abhinav.alfresco.publishing.cloudstore.ConfigReader;

/**
 * Amazon S3 Content Store Implementation
 * {@link org.alfresco.repo.content.ContentStore}.
 *
 * @author Luis Sala
 * @author Updated by - Abhinav Kumar Mishra
 */
public class S3ContentStore extends AbstractContentStore {

	/** The access key. */
	private final String accessKey;
	
	/** The secret key. */
	private final String secretKey;
	
	/** The bucket name. */
	private final String bucketName;

	/** The s3Service. */
	private S3Service s3Service;
	
	/** The bucket. */
	private S3Bucket bucket;

	/** The Constant logger. */
	private static final Log LOG = LogFactory.getLog(S3ContentStore.class);
	
	/**
	 * Initialize an S3 Content Store.
	 */
	public S3ContentStore() {
		
		// Getting the secret keys from properties file.
		final Properties props = ConfigReader.getInstance().getKeys();
		// Amazon Web Services Access Key
		this.accessKey = props.getProperty(CloudStoreConstants.ACCESSKEY);
		// Amazon Web Services Secret Key
		this.secretKey = props.getProperty(CloudStoreConstants.SECRETKEY);
		// Amazon Web Services BucketName
		this.bucketName = props.getProperty(CloudStoreConstants.BUCKET);

		if(LOG.isInfoEnabled()){
			LOG.info("S3ContentStore Initializing: accessKey=" + accessKey
					+ " secretKey=" + secretKey + " bucketName="+ bucketName);
		}
		
		//System.out.println("S3ContentStore Initializing: accessKey="+accessKey+" secretKey="+secretKey+" bucketName="+bucketName);

		// Instantiate S3 Service and create necessary bucket.
		try {
			s3Service = new RestS3Service(new AWSCredentials(accessKey, secretKey));
			if(LOG.isInfoEnabled()){
				LOG.info("S3ContentStore Creating Bucket: bucketName="+ bucketName);
			}
			bucket = s3Service.getOrCreateBucket(bucketName);
			
			if(LOG.isInfoEnabled()){
				LOG.info("S3ContentStore Initialization Complete");
			}
		   //System.out.println("S3ContentStore Initialization Complete");
		} catch (S3ServiceException s3ServExcp) {
			if(LOG.isErrorEnabled()){
				LOG.error("S3ContentStore Initialization Error in Constructor: "+ s3ServExcp);
			}
		}
	}
	
	/**
	 * Initialize an S3 Content Store.
	 *
	 * @param accessKey Amazon Web Services Access Key
	 * @param secretKey Amazon Web Services Secret Key
	 * @param bucketName Name of S3 bucket to store content into.
	 */
	public S3ContentStore(final String accessKey, final String secretKey,
			final String bucketName) {
		this.accessKey = accessKey;
		this.secretKey = secretKey;
		this.bucketName = bucketName;

		if(LOG.isInfoEnabled()){
			LOG.info("S3ContentStore Initializing: accessKey=" + accessKey
					+ " secretKey=" + secretKey + " bucketName="+ bucketName);
		}
		
		//System.out.println("S3ContentStore Initializing, accessKey: "+accessKey+" secretKey: "+secretKey+" and bucketName: "+bucketName);

		// Instantiate S3 Service and create necessary bucket.
		try {
			s3Service = new RestS3Service(new AWSCredentials(accessKey, secretKey));
			if(LOG.isInfoEnabled()){
				LOG.info("S3ContentStore Creating Bucket: bucketName="+ bucketName);
			}
			
			// System.out.println("S3ContentStore Creating Bucket: bucketName="+bucketName);
			bucket = s3Service.getOrCreateBucket(bucketName);
			
			if(LOG.isInfoEnabled()){
				LOG.info("S3ContentStore Initialization Complete");
			}
			
			// System.out.println("S3ContentStore Initialization Complete");
		} catch (S3ServiceException s3ServExcp) {
			if(LOG.isErrorEnabled()){
				LOG.error("S3ContentStore Initialization Error in Constructor: "+ s3ServExcp);
			}
		}
	}

	/**
	 * The main method.<br/>
	 * To test the connection.
	 *
	 * @param args the arguments
	 * @throws S3ServiceException the s3 service exception
	 */
	public static void main(String[] args) throws S3ServiceException {
		final S3Service s3Service = new RestS3Service(new AWSCredentials("xxx", "xxx"));
		final S3Bucket bucket = s3Service.getOrCreateBucket("test_bucket");
		LOG.info("Bucket name: " + bucket.getName());
		LOG.info("S3ContentStore Initialization Complete");
	}

	/* (non-Javadoc)
	 * @see org.alfresco.repo.content.ContentStore#getReader(java.lang.String)
	 */
	public ContentReader getReader(final String contentUrl)
			throws ContentIOException {
		try {
			return new S3ContentReader(contentUrl, s3Service, bucket);
		} catch (Exception globalExcp) {
			throw new ContentIOException(
					"S3ContentStore Failed to get reader for URL: "+ contentUrl, globalExcp);
		}
	}

	/* (non-Javadoc)
	 * @see org.alfresco.repo.content.AbstractContentStore#getWriterInternal(org.alfresco.service.cmr.repository.ContentReader, java.lang.String)
	 */
	public ContentWriter getWriterInternal(
			final ContentReader existingContentReader,
			final String newContentUrl) throws ContentIOException {
		try {
			String contentUrl = null;
			// Was a URL provided?
			if (newContentUrl == null || newContentUrl.equals("")) {
				contentUrl = createNewUrl();
			} else {
				contentUrl = newContentUrl;
			}
			return new S3ContentWriter(contentUrl, existingContentReader, s3Service,bucket);
		}catch (Exception globalExcp) {
			if(LOG.isErrorEnabled()){
				LOG.error("S3ContentStore.getWriterInternal(): Failed to get writer. "+ globalExcp);
			}
			throw new ContentIOException(
					"S3ContentStore.getWriterInternal(): Failed to get writer.");
		}
	}

	/* (non-Javadoc)
	 * @see org.alfresco.repo.content.AbstractContentStore#delete(java.lang.String)
	 */
	public boolean delete(final String contentUrl) throws ContentIOException {
		try {
			if(LOG.isDebugEnabled()){
				LOG.debug("S3ContentStore Deleting Object: contentUrl="+ contentUrl);
			}
			s3Service.deleteObject(bucket, contentUrl);
			return true;
		} catch (S3ServiceException s3ServExcp) {
			if(LOG.isErrorEnabled()){
				LOG.error("S3ContentStore Delete Operation Failed: "+ s3ServExcp);
			}
		} finally {
			cleanup();
		}
		return false;
	} 
	
	/**
	 * Cleanup, connections, buckets, etc. at some point in the future.
	 */
	public void cleanup() {
		//TODO::
	} 

	/* (non-Javadoc)
	 * @see org.alfresco.repo.content.ContentStore#isWriteSupported()
	 */
	public boolean isWriteSupported() {
		return true;
	}
	
	/**
	 * Creates a new content URL.  This must be supported by all
	 * stores that are compatible with Alfresco.
	 *
	 * @return Returns a new and unique content URL
	 */
	public static String createNewUrl() {
		final Calendar calendar = new GregorianCalendar();
		final int year = calendar.get(Calendar.YEAR);
		final int month = calendar.get(Calendar.MONTH) + 1; // 0-based
		final int day = calendar.get(Calendar.DAY_OF_MONTH);
		final int hour = calendar.get(Calendar.HOUR_OF_DAY);
		final int minute = calendar.get(Calendar.MINUTE);
		// create the URL
		final StringBuffer urlStr = new StringBuffer();
		urlStr.append(FileContentStore.STORE_PROTOCOL)
				.append(ContentStore.PROTOCOL_DELIMITER).append(year)
				.append('/').append(month).append('/').append(day).append('/')
				.append(hour).append('/').append(minute).append('/')
				.append(GUID.generate()).append(".bin");
		
		return urlStr.toString();
	}
} 
