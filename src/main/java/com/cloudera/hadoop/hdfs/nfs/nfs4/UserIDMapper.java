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
  
  public abstract int getGIDForGroup(String user, int defaultGID) throws Exception;
  public abstract int getUIDForUser(String user, int defaultUID) throws Exception;

  public abstract String getGroupForGID(int gid, String defaultGroup) throws Exception;
  public abstract String getUserForUID(int gid, String defaultUser) throws Exception;
  
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
    if(user != null) {
      try {
        user = Shell.execCommand("whoami").trim();        
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return user;
  }
}
