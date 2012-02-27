/**
 * Copyright 2011 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.hadoop.hdfs.nfs.nfs4;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Shell;

/**
 * Implementing classes need to map RPC UID's to String usernames
 * and String usernames to UIDs. Same for group information.
 */
public abstract class UserIDMapper {
  
  public abstract int getGIDForGroup(Configuration conf, String user, int defaultGID) throws Exception;
  public abstract int getUIDForUser(Configuration conf, String user, int defaultUID) throws Exception;

  public abstract String getGroupForGID(Configuration conf, int gid, String defaultGroup) throws Exception;
  public abstract String getUserForUID(Configuration conf, int gid, String defaultUser) throws Exception;
  
  protected static Map<Class<?>, UserIDMapper> classUserIdMapperMap = new HashMap<Class<?>, UserIDMapper>();
  
  public static synchronized UserIDMapper get(Configuration conf) {
    boolean cache = conf.getBoolean(USER_ID_MAPPER_CACHE, true);
    Class<?> clazz = conf.getClass(USER_ID_MAPPER_CLASS, UserIDMapperSystem.class, UserIDMapper.class);
    if(cache) {
      UserIDMapper mapper = classUserIdMapperMap.get(clazz);
      if(mapper == null) {
        mapper = (UserIDMapper)ReflectionUtils.newInstance(clazz, conf);
        classUserIdMapperMap.put(clazz, mapper);
      }
      return mapper;      
    }
    return (UserIDMapper)ReflectionUtils.newInstance(clazz, conf);
  }

  /**
   * @return String username associated with the current process
   */
  public static String getCurrentUser() {
    String user = System.getenv("USER");
    if(user == null) {
      try {
        user = Shell.execCommand("whoami").trim();        
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return user;
  }
}
