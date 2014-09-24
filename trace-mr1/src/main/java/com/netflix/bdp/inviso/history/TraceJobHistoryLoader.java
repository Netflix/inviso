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
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapred.JobHistory;
import org.apache.hadoop.mapred.JobHistory.Keys;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;

/**
 * Simple Job History Parser
 *
 * @author dweeks
 */
public class TraceJobHistoryLoader implements JobHistory.Listener {
    private Job job = new Job();
    
    @Override
    public void handle(JobHistory.RecordTypes rt, Map<Keys, String> entries) throws IOException {
        
        switch(rt) {
            case Job:  handleJobEntries(job, entries);
                break;
            case Task: handleTaskEntries(job, entries);
                break;
            case MapAttempt: 
            case ReduceAttempt: handleAttemptEntries(job, entries);
                break;
            case Meta:
                break;
            case Jobtracker:
                break;
            default: throw new RuntimeException("Unknown Record Type: " + rt);
        }

    }
    
    private void handleJobEntries(Job job, Map<Keys, String> entries) {
        for(Entry<Keys, String> e : entries.entrySet()) {
            switch(e.getKey()) {
                case COUNTERS:
                    handleCounterEntries(job, e.getValue());
                    
                    break;
                case MAP_COUNTERS:
                case REDUCE_COUNTERS:
                    break;
                default:
                    String key = e.getKey().name().toLowerCase().trim();
                    key = key.replaceFirst("^job(_)?", "");
                    
                    job.put(key, parseValue(e.getValue()));
            }
        }
    }
    
    private void handleTaskEntries(Job job, Map<Keys, String> entries) {
        Task task = new Task();
        
        for(Entry<Keys, String> e : entries.entrySet()) {
            switch(e.getKey()) {
                case COUNTERS: 
                    handleCounterEntries(task, e.getValue());
                    
                    break;
                default:
                    String key = e.getKey().name().toLowerCase().trim();
                    
                    task.put(key, parseValue(e.getValue()));
            }
        }
        
        String taskId = (String) task.get(Keys.TASKID.name().toLowerCase());
        
        job.getTask(taskId).merge(task);
    }
    
    private void handleAttemptEntries(Job job,  Map<Keys, String> entries) {
        TaskAttempt attempt = new TaskAttempt();
        
        for(Entry<Keys, String> e : entries.entrySet()) {
            switch(e.getKey()) {
                case COUNTERS:
                    handleCounterEntries(attempt, e.getValue());
                    break;
                default:
                    String key = e.getKey().name().toLowerCase().trim();
                    
                    attempt.put(key, parseValue(e.getValue()));
            }
        }
        
        Task task = job.getTask((String) attempt.get(Keys.TASKID.name().toLowerCase()));
        task.getAttempt((String) attempt.get(Keys.TASK_ATTEMPT_ID.name().toLowerCase())).merge(attempt);
    }
    
    private void handleCounterEntries(Map map, String compactString) {
        try {
            Map<String, Map<String, Long>> counters = new HashMap<>();
            
            for(Group group : Counters.fromEscapedCompactString(compactString)) {
                Map<String,Long> cmap = new HashMap<>();
                
                for(Counter counter : group) {
                    cmap.put(counter.getDisplayName(), counter.getCounter());
                }
                
                counters.put(group.getDisplayName(), cmap);
            }
            
            map.put("counters", counters);
        } catch (ParseException ex) {
            Logger.getLogger(TraceJobHistoryLoader.class.getName()).log(Level.SEVERE, null, ex);
        }
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

}
