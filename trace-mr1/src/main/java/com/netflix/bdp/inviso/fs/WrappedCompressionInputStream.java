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
import java.io.InputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.compress.CompressionInputStream;

/**
 *
 * @author dweeks
 */
public class WrappedCompressionInputStream extends CompressionInputStream implements PositionedReadable {

    public WrappedCompressionInputStream(CompressionInputStream in) throws IOException {
        super(in);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return ((CompressionInputStream)this.in).read(b, off, len);
    }

    @Override
    public void resetState() throws IOException {
        ((CompressionInputStream)this.in).resetState();
    }

    @Override
    public int read() throws IOException {
        return ((CompressionInputStream)this.in).read();
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }


}
