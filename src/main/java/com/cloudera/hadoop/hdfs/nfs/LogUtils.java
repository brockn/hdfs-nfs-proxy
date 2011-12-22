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
