/*
 *
 *  Copyright 2014 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.netflix.bdp.inviso.history.impl;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.netflix.bdp.inviso.history.HistoryLocator;

public class BucketedHistoryLocator implements HistoryLocator {
	private Configuration conf;

	private Path bucketedPath;
	private String historyPostfix;
	private String configPostfix;
	
	@Override
	public void initialize(Configuration config) throws Exception {
		this.conf = config;
		
		bucketedPath = new Path(conf.get("inviso.history.location", "hdfs://tmp"));
		configPostfix = conf.get("inviso.history.location.postfix", ".conf.gz");
		historyPostfix = conf.get("inviso.history.location.postfix", ".history.gz");
	}

	/**
	 * Returns the config and history locations
     * @param jobId
     * @return 
	 */
	@Override
	public Pair<Path, Path> locate(String jobId) {
		String bucket = jobId.substring(jobId.length() - conf.getInt("inviso.history.bucket.depth", 3));
		
		Path originConfig = new Path(bucketedPath +Path.SEPARATOR+  bucket, jobId + configPostfix);
		Path originHistory = new Path(bucketedPath +Path.SEPARATOR+ bucket, jobId + historyPostfix);
	
        return new ImmutablePair<>(originConfig, originHistory);
	}

	@Override
	public void close() throws Exception { }

}
