package com.cloudera.hadoop.hdfs.nfs;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import java.net.InetAddress;

import org.apache.hadoop.conf.Configuration;

public class NFSUtils {

  public static String getDomain(Configuration conf, InetAddress address) {
    String override = conf.get(NFS_OWNER_DOMAIN);
    if(override != null) {
      return override;
    }
    String host = address.getCanonicalHostName();
    if(address.isLoopbackAddress() && 
        address.getHostAddress().equals(address.getHostName())) {
      // loopback does not resolve
      return "localdomain";
    }
    int pos;
    if((pos = host.indexOf('.')) > 0 && pos < host.length()) {
      return host.substring(pos + 1);
    }
    return host;
  }

}
