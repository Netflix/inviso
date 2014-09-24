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

import com.netflix.bdp.inviso.history.job.Job;
import com.netflix.bdp.inviso.history.job.Task;
import com.netflix.bdp.inviso.history.job.TaskAttempt;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.jobhistory.HistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.HistoryEventHandler;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig.Feature;

/**
 * Basic handler for loading history file.
 *
 * @author dweeks
 */
public class TraceJobHistoryLoader implements HistoryEventHandler {
    private static final org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(TraceJobHistoryLoader.class.getName());
    private static final Map<String,String> COUNTER_TAGS = new HashMap<>();
    
    static {
        COUNTER_TAGS.put("counters", "getCounters");
        COUNTER_TAGS.put("mapCounters", "getMapCounters");
        COUNTER_TAGS.put("reduceCounters", "getReduceCounters");
        COUNTER_TAGS.put("totalCounters", "getTotalCounters");
    }
    
    private Set<String> skipElements = new HashSet<>();
    private Job job = new Job();

    public TraceJobHistoryLoader(PropertiesConfiguration properties) {
        skipElements.addAll(properties.getList("inviso.trace.skip.elements"));
    }
    
    @Override
    public void handleEvent(HistoryEvent event) throws IOException {
        try {
            
            switch (event.getEventType()) {
                case AM_STARTED:
                case JOB_SUBMITTED:
                case JOB_STATUS_CHANGED:
                case JOB_PRIORITY_CHANGED:
                case JOB_INITED:
                case JOB_INFO_CHANGED:
                case JOB_FINISHED:
                    /* History doesn't have an event for success, so we need to derive
                       the successful state.
                    */
                    job.put("jobStatus", JobStatus.State.SUCCEEDED.toString());
                case JOB_FAILED:
                case JOB_ERROR:
                case JOB_KILLED:
                    handleJobEvent(event);
                    break;

                //Task Events
                case TASK_STARTED:
                case TASK_UPDATED:
                case TASK_FINISHED:
                case TASK_FAILED:
                    handleTaskEvent(event);
                    break;
                    
                case SETUP_ATTEMPT_STARTED:    
                case SETUP_ATTEMPT_FAILED:
                case SETUP_ATTEMPT_FINISHED:
                case SETUP_ATTEMPT_KILLED:
                case CLEANUP_ATTEMPT_STARTED:
                case CLEANUP_ATTEMPT_FINISHED:
                case CLEANUP_ATTEMPT_KILLED:
                case CLEANUP_ATTEMPT_FAILED:
                case MAP_ATTEMPT_STARTED:
                case MAP_ATTEMPT_FINISHED:
                case MAP_ATTEMPT_KILLED:
                case MAP_ATTEMPT_FAILED:
                case REDUCE_ATTEMPT_STARTED:
                case REDUCE_ATTEMPT_FINISHED:
                case REDUCE_ATTEMPT_KILLED:
                case REDUCE_ATTEMPT_FAILED:
                    handleAttemptEvent(event);
                    break;

                default: log.info("Ignoring event: " + event);
            }
        } catch (IllegalArgumentException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            log.error("", e);
        }
    }

    private void handleJobEvent(HistoryEvent event) throws IllegalArgumentException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        for(Field f : event.getDatum().getClass().getFields()) {
            f.setAccessible(true);
            
            if(Modifier.isStatic(f.getModifiers())) {
                continue;
            }
            
            String name = f.getName();
            Object value = f.get(event.getDatum());
            
            if(skipElements.contains(name)) {
                continue;
            }
            
            if(value instanceof CharSequence) {
                value = value.toString();
            }
            
            if(value == null || value.toString().trim().isEmpty()) {
                continue;
            }
            
            if(COUNTER_TAGS.containsKey(name)) {
                Method m = event.getClass().getDeclaredMethod(COUNTER_TAGS.get(name), new Class[0]);
                m.setAccessible(true);
                
                Counters counters = (Counters) m.invoke(event, new Object[0]);
                value = handleCounterEntries(counters);
            }
            
            job.put(name, value);
        }
    }
    
    private void handleTaskEvent(HistoryEvent event) throws IllegalArgumentException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        Task task = new Task();
        
        for(Field f : event.getDatum().getClass().getFields()) {
            f.setAccessible(true);
            
            if(Modifier.isStatic(f.getModifiers())) {
                continue;
            }
            
            String name = f.getName();
            Object value = f.get(event.getDatum());
            
            if(skipElements.contains(name)) {
                continue;
            }
            
            if(value instanceof CharSequence) {
                value = value.toString();
            }
            
            if(value == null || value.toString().trim().isEmpty()) {
                continue;
            }
            
            if("counters".equals(name)) {
                Method m = event.getClass().getDeclaredMethod("getCounters", new Class[0]);
                m.setAccessible(true);
                
                Counters counters = (Counters) m.invoke(event, new Object[0]);
                value = handleCounterEntries(counters);
            }
            
            task.put(name, value);
        }
        
        String taskId = (String) task.get("taskid");
        
        job.getTask(taskId).merge(task);
    }
    
    private void handleAttemptEvent(HistoryEvent event) throws IllegalArgumentException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        TaskAttempt attempt = new TaskAttempt();
        
        for(Field f : event.getDatum().getClass().getFields()) {
            f.setAccessible(true);
            
            if(Modifier.isStatic(f.getModifiers())) {
                continue;
            }
            
            String name = f.getName();
            Object value = f.get(event.getDatum());
            
            if(skipElements.contains(name)) {
                continue;
            }
            
            if(value instanceof CharSequence) {
                value = value.toString();
            }
            
            if(value == null || value.toString().trim().isEmpty()) {
                continue;
            }
            
            if("counters".equals(name)) {
                Method m = event.getClass().getDeclaredMethod("getCounters", new Class[0]);
                m.setAccessible(true);
                
                Counters counters = (Counters) m.invoke(event, new Object[0]);
                value = handleCounterEntries(counters);
            }
            
            attempt.put(name, value);
        }
        
        Task task = job.getTask("taskid");
        task.getAttempt((String) attempt.get("attemptId")).merge(attempt);
    }
    
    private Map<String, Map<String, Long>> handleCounterEntries(Counters counters) {
        Map<String, Map<String, Long>> result = new HashMap<>();
        
        for(CounterGroup group : counters) {    
            Map<String,Long> cmap = new HashMap<>();

            for(Counter counter : group) {
                cmap.put(counter.getDisplayName(), counter.getValue());
            }

            result.put(group.getDisplayName(), cmap);
        }
        
        return result;
    }
    
    private Object parseValue(String value) {
        Object result = value;
        
        if(NumberUtils.isDigits((String) value)) {
            result = Long.parseLong(value);
        } else if(NumberUtils.isNumber((String) value)) {
            result = Double.parseDouble(value);
        }
        
        return result;
    }

    public Job getJob() {
        return job;
    }
    
    public static void main(String[] args) throws Exception {
        FileSystem fs = FileSystem.newInstanceLocal(new Configuration());
        
        JobHistoryParser parser = new JobHistoryParser(fs, "/tmp/job_1405808155709_124465.history");
        
        //JobInfo jobInfo = parser.parse();
        TraceJobHistoryLoader loader = new TraceJobHistoryLoader(new PropertiesConfiguration());
        parser.parse(loader);
        
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(Feature.INDENT_OUTPUT, true);
        mapper.writeValue(new File("/tmp/mr2-hist.json"), loader.getJob());
    }

}
