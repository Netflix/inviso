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


package com.netflix.bdp.inviso.history.job.json;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.MapSerializer;
import com.fasterxml.jackson.databind.type.MapLikeType;
import com.fasterxml.jackson.databind.type.SimpleType;
import com.netflix.bdp.inviso.history.job.Job;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;


/**
 *
 * @author dweeks
 */
public class FilteredMapSerializer{

    public FilteredMapSerializer() {

    }

    public static void main(String[] args) throws Exception {
        
        Job job = new Job();
        String [] ignore = { "three" };
        
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("m1", "v1");
        map.put("m2", "v2");
        
        job.put("one", 1);
        job.put("two", "two");
        job.put("three", map);
        
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule("MyModule", new Version(1, 0, 0, null));
        JavaType mapType = MapLikeType.construct(Job.class, SimpleType.construct(String.class), SimpleType.construct(Object.class));        
        module.addSerializer(Job.class, MapSerializer.construct(ignore, mapType, false, null, null, null, null));
        //module.addSerializer(Job.class, new MapSerializer(ignore, mapType, false, null, null, null, null));
        mapper.registerModule(module);
        
        System.out.println(mapper.writeValueAsString(job));
    }
}
