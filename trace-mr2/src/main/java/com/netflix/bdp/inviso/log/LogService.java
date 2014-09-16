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


package com.netflix.bdp.inviso.log;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import javax.ws.rs.GET;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogKey;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogReader;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;

/**
 *
 * @author dweeks
 */
@javax.ws.rs.Path("log")
public class LogService {

    @javax.ws.rs.Path("load/{owner}/{appId}/{containerId}/{nodeId}")
    @GET
    @Produces("text/plain")
    public Response log(@PathParam("owner") String owner, 
                        @PathParam("appId") String appId, 
                        @PathParam("containerId") String containerId, 
                        @PathParam("nodeId") String nodeId,
                        @QueryParam("fs") String fs, 
                        @QueryParam("root") String root) throws IOException {
        
        Configuration conf = new Configuration();
        
        if(fs != null) {
            conf.set("fs.default.name", fs);
        }
        
        Path logRoot = new Path(conf.get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR, YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR));
        
        if(root != null) {
            logRoot = new Path(root);
        }
        
        Path logPath = LogAggregationUtils.getRemoteNodeLogFileForApp(
                logRoot,
                ConverterUtils.toApplicationId(appId),
                owner,
                ConverterUtils.toNodeId(nodeId),
                LogAggregationUtils.getRemoteNodeLogDirSuffix(conf)
        );
        
        AggregatedLogFormat.LogReader reader = new AggregatedLogFormat.LogReader(conf, logPath);
        
        LogKey key = new LogKey();
        
        DataInputStream in = reader.next(key);
        
        while(in != null && !key.toString().equals(containerId)) {
            key = new LogKey();
            in = reader.next(key);
        }
        
        if(in == null) {
            throw new WebApplicationException(404);
        }
        
        final DataInputStream fin = in;
        
        StreamingOutput stream = new StreamingOutput() {
            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {
                PrintStream out = new PrintStream(os);
                
                while(true) {
                    try {
                        LogReader.readAContainerLogsForALogType(fin, out);
                        out.flush();
                    } catch (EOFException e) {
                        break;
                    }
                }
            }
        };
        
        return Response.ok(stream).build();
    }
}


