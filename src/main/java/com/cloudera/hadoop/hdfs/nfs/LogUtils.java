package com.cloudera.hadoop.hdfs.nfs;


import java.lang.reflect.Field;
import java.util.List;

import com.cloudera.hadoop.hdfs.nfs.nfs4.MessageBase;

public class LogUtils {

  @SuppressWarnings("rawtypes")
  public static String dump(Object parent) {
    StringBuilder buffer = new StringBuilder();
    Field[] fields = parent.getClass().getDeclaredFields();
    buffer.append("START").append(parent.getClass().getName()).append(" = '").append(parent).append("' = ");
    buffer.append(fields.length).append("\n");
    for(Field field : fields) {
      field.setAccessible(true);
      try {
        Object child = field.get(parent);
        if(child instanceof List) {
          for(Object grandChild : (List)child) {
            buffer.append(dump(grandChild));
          }
        } else if (child instanceof MessageBase){ 
          buffer.append(dump(child));
        } else {
            buffer.append(field.getType().getSimpleName()).append(" ").append(field.getName());
            buffer.append(" = '").append(child).append("'\n");
        }
      } catch (IllegalAccessException e) {
        buffer.append("IllegalAccessException: ").append(e.getMessage()).append("\n");
      }        
    }
    return buffer.append("END ").append(parent.getClass().getName()).append("\n").toString();
  }
}
