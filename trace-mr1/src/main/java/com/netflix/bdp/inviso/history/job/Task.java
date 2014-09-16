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


package com.netflix.bdp.inviso.history.job;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author dweeks
 */
public class Task extends HashMap<String, Object> {
    private static final String ATTEMPTS = "attempts";
    private Map<String, TaskAttempt> attempts = new HashMap<>();
    
    public Task() {
        this.put(ATTEMPTS, attempts);
    }

    public void merge(Task other) {
        this.attempts.putAll((Map<String, TaskAttempt>) other.remove(ATTEMPTS));
        
        this.putAll(other);
    }
    
    public TaskAttempt getAttempt(String attemptId) {
        TaskAttempt attempt = this.attempts.get(attemptId);
        
        if(attempt == null) {
            attempt = new TaskAttempt();
            this.attempts.put(attemptId, attempt);
        }
        
        return attempt;
    }
    
}
