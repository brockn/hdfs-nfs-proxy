/**
 * Copyright 2012 The Apache Software Foundation
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
  
  public ClientHostsMatcher(String hosts) {
    mMatches = Lists.newArrayList();
    for(String host : hosts.split("\\s+")) {
      mMatches.add(getMatch(host));
    }
  }
  public boolean isIncluded(String address, String hostname) {
    for(Match match : mMatches) {
      if(match.isIncluded(address, hostname)) {
        return true;
      }
    }
    return false;
  }
  
  private static abstract class Match {    
    public abstract boolean isIncluded(String address, String hostname);
  }
  private static class AnonymousMatch extends Match {
    @Override
    public boolean isIncluded(String ip, String hostname) {
      return true;
    }
  }
  private static class CIDRMatch extends Match {
    private final SubnetInfo subnetInfo;
    private CIDRMatch(SubnetInfo subnetInfo) {
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
    private ExactMatch(String ipOrHost) {
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
    private RegexMatch(String wildcard) {
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
 
  private static Match getMatch(String host) {
   host = host.toLowerCase().trim();
   if(host.equals("*")) {
     LOGGER.debug("Using match all for '" + host + "'");
     return new AnonymousMatch();
   } else if(CIDR_FORMAT_SHORT.matcher(host).matches()) {
     LOGGER.debug("Using CIDR match for '" + host + "'");
     return new CIDRMatch(new SubnetUtils(host).getInfo());
   } else if(CIDR_FORMAT_LONG.matcher(host).matches()) {
     LOGGER.debug("Using CIDR match for '" + host + "'");
     String[] pair = host.split("/");
     return new CIDRMatch(new SubnetUtils(pair[0], pair[1]).getInfo());
   } else if(host.contains("*") || host.contains("?") || 
       host.contains("[") || host.contains("]")) {
     LOGGER.debug("Using Regex match for '" + host + "'");
     return new RegexMatch(host);
   }
   LOGGER.debug("Using exact match for '" + host + "'");
   return new ExactMatch(host);
  }
}
