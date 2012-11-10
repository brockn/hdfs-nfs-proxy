/**
 * Copyright 2012 Cloudera Inc.
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
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;

import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.nfs4.MessageBase;

public class LogUtils {
  protected static final Logger LOGGER = Logger.getLogger(LogUtils.class);
  @SuppressWarnings("rawtypes")
  public static String dump(Object parent) {
    StringBuilder buffer = new StringBuilder();
    Field[] fields = parent.getClass().getDeclaredFields();
    buffer.append("START ").append(parent.getClass().getName()).append(" = '").append(parent).append("' = ");
    buffer.append(fields.length).append("\n");
    try {
      for(Method method : parent.getClass().getMethods()) {
        int mod = method.getModifiers();
        if (method.getName().startsWith("get") && method.getParameterTypes().length == 0
            && !(Modifier.isStatic(mod) || Modifier.isAbstract(mod) || Modifier.isNative(mod))) {
          Object result = method.invoke(parent, (Object[]) null);
          if(result instanceof List) {
            for(Object grandChild : (List)result) {
              buffer.append(dump(grandChild));
            }
          } else if (result instanceof MessageBase){
            buffer.append(dump(result));
          } else {
            buffer.append(method.getDeclaringClass().getSimpleName()).append(" ").append(method.getName());
            buffer.append(" = '").append(result).append("'\n");
          }
        }
      }
    } catch(Exception ex) {
      LOGGER.info("Error performing debug logging", ex);
    }
    return buffer.append("END ").append(parent.getClass().getName()).append("\n").toString();
  }
}
