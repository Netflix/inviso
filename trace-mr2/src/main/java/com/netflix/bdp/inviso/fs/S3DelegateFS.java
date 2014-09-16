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


package com.netflix.bdp.inviso.fs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;

/**
 *
 * @author dweeks
 */
public class S3DelegateFS extends DelegateToFileSystem {

    public S3DelegateFS(URI uri, Configuration conf) throws IOException, URISyntaxException {
        super(uri, new NativeS3FileSystem(), conf, "s3", false);
    }

    public S3DelegateFS(URI theUri, FileSystem theFsImpl, Configuration conf, String supportedScheme, boolean authorityRequired) throws IOException, URISyntaxException {
        super(theUri, theFsImpl, conf, supportedScheme, authorityRequired);
    }

    @Override
    public void checkPath(Path path) {
        //bypass
    }
    
    
}
