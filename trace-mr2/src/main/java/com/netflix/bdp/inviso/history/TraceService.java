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

package com.netflix.bdp.inviso.history;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.MapSerializer;
import com.fasterxml.jackson.databind.type.MapLikeType;
import com.fasterxml.jackson.databind.type.SimpleType;
import com.netflix.bdp.inviso.fs.WrappedCompressionInputStream;
import com.netflix.bdp.inviso.history.job.Job;
import com.netflix.bdp.inviso.history.job.Task;
import com.netflix.bdp.inviso.history.job.TaskAttempt;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import net.sf.ehcache.CacheManager;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser;
import org.apache.log4j.Logger;


/**
 *
 * @author dweeks
 */
@Path("trace")
public class TraceService implements ServletContextListener {
    private static final Logger log = Logger.getLogger(TraceService.class.getName());
    
    private static Configuration config;
    
    private static HistoryLocator historyLocator;
    
    private static PropertiesConfiguration properties;
    
    @Override
    public void contextInitialized(ServletContextEvent context) {
        log.info("Initializing Trace Service");
        
        config = new Configuration();
        
        properties = new PropertiesConfiguration();
        try {
            properties.load(TraceService.class.getClassLoader().getResourceAsStream("trace.properties"));
            
            Class<?> c = config.getClass("trace.history.locator.impl", com.netflix.bdp.inviso.history.impl.BucketedHistoryLocator.class);
            historyLocator = (HistoryLocator) c.newInstance();
            historyLocator.initialize(config);
        } catch (Exception e) {
            log.error("Failed to initialize trace service.",e);
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        CacheManager.getInstance().shutdown();
       
        try {
            historyLocator.close();
        } catch (Exception e) {
            log.error("Failed to close properly.", e);
        }
        
        log.info("Trace Service Destroyed");
    }
  
    @Path("load/{jobId}")
    @GET
    @Produces("application/json")
    public String trace(@PathParam("jobId") final String jobId, @QueryParam("path") final String path, @QueryParam("summary") boolean summary, @QueryParam("counters") @DefaultValue("true") boolean counters) throws Exception {
        
        Pair<org.apache.hadoop.fs.Path, org.apache.hadoop.fs.Path> historyPath;
        
        if(path != null) {
            historyPath = new ImmutablePair<>(null, new org.apache.hadoop.fs.Path(path));
        } else {
            historyPath = historyLocator.locate(jobId);
        }
        
        if(historyPath == null) {
            throw new WebApplicationException(404);
        }
        
        TraceJobHistoryLoader loader = new TraceJobHistoryLoader(properties);
        
        FileSystem fs = FileSystem.get(historyPath.getRight().toUri(), config);
        CompressionCodec codec = new CompressionCodecFactory(config).getCodec(historyPath.getRight());
        
        FSDataInputStream fin = fs.open(historyPath.getRight());
        
        if(codec != null) {
            fin = new FSDataInputStream(new WrappedCompressionInputStream(codec.createInputStream(fin)));
        }
        
        JobHistoryParser parser = new JobHistoryParser(fin);
        parser.parse(loader);
        
        String [] ignore = { "counters" };
        
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule("MyModule", new Version(1, 0, 0, null));
        
        //Job
        JavaType jobMapType = MapLikeType.construct(Job.class, SimpleType.construct(String.class), SimpleType.construct(Object.class));        
        module.addSerializer(Job.class, MapSerializer.construct(ignore, jobMapType, false, null, null, null, null));
        
        //Task
        JavaType taskMapType = MapLikeType.construct(Task.class, SimpleType.construct(String.class), SimpleType.construct(Object.class));        
        module.addSerializer(Task.class, MapSerializer.construct(ignore, taskMapType, false, null, null, null, null));
        
        //Attempt
        JavaType attemptMapType = MapLikeType.construct(TaskAttempt.class, SimpleType.construct(String.class), SimpleType.construct(Object.class));        
        module.addSerializer(TaskAttempt.class, MapSerializer.construct(ignore, attemptMapType, false, null, null, null, null));
        
        if(!counters) {
            mapper.registerModule(module);
        }
        
        if(summary) {
            loader.getJob().clearTasks();
        }
        
        return mapper.writeValueAsString(loader.getJob());
    }
    
    @Path("counters/{jobId}")
    @GET
    @Produces("application/json")
    public String counters(@PathParam("jobId") final String jobId) {
    	
    	
    	return null;
    }
    
    @Path("archive/{jobId}")
    @PUT
    @Produces("application/json")
    public void archive() {
        
    }
}
