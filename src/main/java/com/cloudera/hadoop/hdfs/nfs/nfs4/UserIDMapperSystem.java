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
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;
import org.apache.log4j.Logger;

/**
 * Implements UID -&gt; User and User -&gt; UID mapping for Linux.
 */
public class UserIDMapperSystem extends UserIDMapper {
  private static final Logger LOGGER = Logger.getLogger(UserIDMapperSystem.class);

  public UserIDMapperSystem() {
    this(DEFAULT_NEGATIVE_CACHE, DEFAULT_POSITIVE_CACHE);
  }
  /**
   * Times are in milliseconds
   * 
   * @param negativeCacheTime
   * @param positiveCacheTime
   */
  public UserIDMapperSystem(long negativeCacheTime, long positiveCacheTime) {
    this.mNegativeCacheTime = negativeCacheTime;
    this.mPositiveCacheTime = positiveCacheTime;
  }
  
  /** a Unix command to get a given user's uid */
  protected static String[] getUIDForUserCommand(final String user) {
    return new String [] {"bash", "-c", "id -u " + user};
  }
  /** a Unix command to get a given user's gid */
  protected static String[] getGIDForUserCommand(final String group) {
    return new String [] {"bash", "-c", "id -g " + group};
  }


  
  public String getGroupForGID(Configuration conf, int gid, String group) throws Exception {
    return getCachedUserGroup("/etc/group", gid, group, mNegativeGROUPCache, mPositiveGROUPCache);
  }
  public String getUserForUID(Configuration conf, int uid, String user) throws Exception {
    if(uid == ROOT_USER_UID) {
      String superUser = getCurrentUser();
      LOGGER.info("Mapping root to '" + superUser + "'");
      return superUser;
    }
    return getCachedUserGroup("/etc/passwd", uid, user, mNegativeUSERCache, mPositiveUSERCache);

  }


  @Override
  public int getGIDForGroup(Configuration conf, String group, int defaultGID) throws Exception {
    int id = getCachedID(getGIDForUserCommand(group), group, defaultGID, mNegativeGIDCache, mPositiveGIDCache);
    LOGGER.info("getGIDForGroup: Looking for " + group + " and got " + id);
    return id;
  }

  @Override
  public int getUIDForUser(Configuration conf, String user, int defaultUID) throws Exception {
    int id = getCachedID(getUIDForUserCommand(user), user, defaultUID, mNegativeUIDCache, mPositiveUIDCache);
    LOGGER.info("getUIDForUser: Looking for " + user + " and got " + id);
    return id;
  }
  
  protected synchronized int getCachedID(String[] cmd, String name, int defaultID, 
      Map<String, Long> negativeCache, Map<String, IDCache<Integer>> postitveCache) throws Exception {
    Long timestamp = negativeCache.get(name);
    if(timestamp != null) {
      if(System.currentTimeMillis() - timestamp < mNegativeCacheTime) {
        return defaultID;
      } else {
        negativeCache.remove(name);
      }
    }
    
    IDCache<Integer> cache = postitveCache.get(name);
    if(cache != null) {
      if(System.currentTimeMillis() - cache.timestamp < mPositiveCacheTime) {
        return cache.id;
      } else {
        postitveCache.remove(name);
      }
    }
    
    
    String result =  null;
    try {
      result = Shell.execCommand(cmd);
      int id = Integer.parseInt(result.trim());
      postitveCache.put(name, new IDCache<Integer>(id));
      return id;
    } catch (Shell.ExitCodeException e) {
      LOGGER.warn("Error parsing '" + result + "' for name '" + name + "': " + e.getMessage().trim());
    }
    negativeCache.put(name, System.currentTimeMillis());
    return defaultID;
  }
  
  protected synchronized String getCachedUserGroup(String idFile, int id, String defaultID, 
      Map<Integer, Long> negativeCache, Map<Integer, IDCache<String>> postitveCache) throws Exception {
    Long timestamp = negativeCache.get(id);
    if(timestamp != null) {
      if(System.currentTimeMillis() - timestamp < mNegativeCacheTime) {
        return defaultID;
      } else {
        negativeCache.remove(id);
      }
    }
    
    IDCache<String> cache = postitveCache.get(id);
    if(cache != null) {
      if(System.currentTimeMillis() - cache.timestamp < mPositiveCacheTime) {
        return cache.id;
      } else {
        postitveCache.remove(id);
      }
    }
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new InputStreamReader(new FileInputStream(idFile)));
      String line;
      Pattern pattern = Pattern.compile("^([A-z]+):x:" + id + ":");    
      while((line = reader.readLine()) != null) {
        Matcher matcher = pattern.matcher(line);
        if(matcher.find()) {
          String name = matcher.group(1);
          postitveCache.put(id, new IDCache<String>(name));
          return name;
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Error parsing "+idFile+" for name '" + id + "'", e);
    } finally {
      if(reader != null) {
        try {
          reader.close();
        } catch (Exception x) {
          LOGGER.info("Error closing reader", x);
        }
      }
    }
    negativeCache.put(id, System.currentTimeMillis());
    return defaultID;
  }  
  
  class IDCache<K> {
    K id;
    long timestamp;
    public IDCache(K id) {
      this.id = id;
      this.timestamp = System.currentTimeMillis();
    }
  }
  
  /*
   * This class has two caches, negative (errors) and positive. Because
   * Creating sub processes is expensive, we can this for some time. 
   * 
   * Good idea? Well, if you changed the UID of a user which has files
   * in HDFS as well, it would take a little time for this class to be
   * notified. Tradeoffs....
   */
  protected Map<String, Long> mNegativeUIDCache = new HashMap<String, Long>();
  protected Map<String, Long> mNegativeGIDCache = new HashMap<String, Long>(); 
  protected Map<String, IDCache<Integer>> mPositiveUIDCache = new HashMap<String, IDCache<Integer>>();
  protected Map<String, IDCache<Integer>> mPositiveGIDCache = new HashMap<String, IDCache<Integer>>();

  protected Map<Integer, Long> mNegativeUSERCache = new HashMap<Integer, Long>();
  protected Map<Integer, Long> mNegativeGROUPCache = new HashMap<Integer, Long>(); 
  protected Map<Integer, IDCache<String>> mPositiveUSERCache = new HashMap<Integer, IDCache<String>>();
  protected Map<Integer, IDCache<String>> mPositiveGROUPCache = new HashMap<Integer, IDCache<String>>();

  protected long mNegativeCacheTime;
  protected long mPositiveCacheTime;
  
  protected static final long DEFAULT_NEGATIVE_CACHE = 1000L * 60L * 5L; // 5min
  protected static final long DEFAULT_POSITIVE_CACHE = 1000L * 60L * 1L; // 60 seconds
  
  public static void main(String[] args) throws Exception {
    UserIDMapperSystem mapper = new UserIDMapperSystem();
    System.out.println(mapper.getUserForUID(new Configuration(), 500, "NOT FOUND"));
    System.out.println(mapper.getGroupForGID(new Configuration(), 500, "NOT FOUND"));

  }
}
