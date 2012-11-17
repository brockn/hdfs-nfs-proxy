/**
 * Copyright 2012 Cloudera Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.cloudera.hadoop.hdfs.nfs.security;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.net.util.SubnetUtils;
import org.apache.commons.net.util.SubnetUtils.SubnetInfo;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

public class ClientHostsMatcher {
  private static final Logger LOGGER = Logger.getLogger(ClientHostsMatcher.class);
  private static final String IP_ADDRESS = "(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})";
  private static final String SLASH_FORMAT_SHORT = IP_ADDRESS + "/(\\d{1,3})";
  private static final String SLASH_FORMAT_LONG = IP_ADDRESS + "/" + IP_ADDRESS;
  private static final Pattern CIDR_FORMAT_SHORT = Pattern.compile(SLASH_FORMAT_SHORT);
  private static final Pattern CIDR_FORMAT_LONG = Pattern.compile(SLASH_FORMAT_LONG);

  private final List<Match> mMatches;
  
  public ClientHostsMatcher(String lines) {
    mMatches = Lists.newArrayList();
    for(String line : lines.split("\\n")) {
      LOGGER.debug("Processing line '" + line + "'");
      line = line.trim();
      if(!line.isEmpty()) {
        mMatches.add(getMatch(line));        
      }
    }
  }
  public AccessPrivilege getAccessPrivilege(String address, String hostname) {
    for(Match match : mMatches) {
      if(match.isIncluded(address, hostname)) {
        return match.accessPrivilege;
      }
    }
    return AccessPrivilege.NONE;
  }
  
  private static abstract class Match {
    private final AccessPrivilege accessPrivilege;
    private Match(AccessPrivilege accessPrivilege) {
      this.accessPrivilege = accessPrivilege;
    }
    public abstract boolean isIncluded(String address, String hostname);
  }
  private static class AnonymousMatch extends Match {
    private AnonymousMatch(AccessPrivilege accessPrivilege) {
      super(accessPrivilege);
    }
    @Override
    public boolean isIncluded(String ip, String hostname) {
      return true;
    }
  }
  private static class CIDRMatch extends Match {
    private final SubnetInfo subnetInfo;
    private CIDRMatch(AccessPrivilege accessPrivilege, SubnetInfo subnetInfo) {
      super(accessPrivilege);
      this.subnetInfo = subnetInfo;
    }
    @Override
    public boolean isIncluded(String address, String hostname) {
      if(subnetInfo.isInRange(address)) {
        return true;
      }
      if(LOGGER.isDebugEnabled()) {
        LOGGER.debug("CIDRNMatcher low = " + subnetInfo.getLowAddress() + 
            ", high = " + subnetInfo.getHighAddress() + 
            ", denying client '" + address + "', '" + hostname + "'");        
      }
      return false;
    }    
  }
  private static class ExactMatch extends Match {
    private String ipOrHost;
    private ExactMatch(AccessPrivilege accessPrivilege, String ipOrHost) {
      super(accessPrivilege);
      this.ipOrHost = ipOrHost;
    }
    @Override
    public boolean isIncluded(String address, String hostname) {
      if(ipOrHost.equalsIgnoreCase(address) || 
          ipOrHost.equalsIgnoreCase(hostname)) {
        return true;
      }
      if(LOGGER.isDebugEnabled()) {
        LOGGER.debug("ExactMatcher '" + ipOrHost + "', denying  client " +
        		"'" + address + "', '" + hostname + "'");        
      }
      return false;
    }    
  }
  
  private static class RegexMatch extends Match {
    private final Pattern pattern;
    private RegexMatch(AccessPrivilege accessPrivilege, String wildcard) {
      super(accessPrivilege);
      this.pattern = Pattern.compile(wildcard, Pattern.CASE_INSENSITIVE);
    }
    @Override
    public boolean isIncluded(String address, String hostname) {
      if(pattern.matcher(address).matches() || 
          pattern.matcher(hostname).matches()) {
        return true;
      }
      if(LOGGER.isDebugEnabled()) {
        LOGGER.debug("RegexMatcher '" + pattern.pattern() + 
            "', denying client '" + address + "', '" + hostname + "'");        
      }
      return false;
    }    
  }
 
  private static Match getMatch(String line) {
   String[] parts = line.split("\\s+");
   String host;
   AccessPrivilege privilege = AccessPrivilege.READ_ONLY;
   switch(parts.length) {
   case 1:
     host = parts[0];
     break;
   case 2:
     host = parts[0];
     String option = parts[1].trim();
     if("rw".equalsIgnoreCase(option)) {
       privilege = AccessPrivilege.READ_WRITE;
     }
     break;
   default:
       throw new IllegalArgumentException("Incorrectly formatted line '" + line + "'");
   }
   host = host.toLowerCase().trim();
   if(host.equals("*")) {
     LOGGER.debug("Using match all for '" + host + "' and " + privilege);
     return new AnonymousMatch(privilege);
   } else if(CIDR_FORMAT_SHORT.matcher(host).matches()) {
     LOGGER.debug("Using CIDR match for '" + host + "' and " + privilege);
     return new CIDRMatch(privilege, new SubnetUtils(host).getInfo());
   } else if(CIDR_FORMAT_LONG.matcher(host).matches()) {
     LOGGER.debug("Using CIDR match for '" + host + "' and " + privilege);
     String[] pair = host.split("/");
     return new CIDRMatch(privilege, new SubnetUtils(pair[0], pair[1]).getInfo());
   } else if(host.contains("*") || host.contains("?") || 
       host.contains("[") || host.contains("]")) {
     LOGGER.debug("Using Regex match for '" + host + "' and " + privilege);
     return new RegexMatch(privilege, host);
   }
   LOGGER.debug("Using exact match for '" + host + "' and " + privilege);
   return new ExactMatch(privilege, host);
  }
}
