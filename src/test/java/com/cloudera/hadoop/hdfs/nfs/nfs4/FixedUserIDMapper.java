package com.cloudera.hadoop.hdfs.nfs.nfs4;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class FixedUserIDMapper extends UserIDMapper {

  @Override
  public int getGIDForGroup(Configuration conf, String user, int defaultGID)
      throws Exception {
    return Integer.parseInt(Shell.execCommand("id -g").trim());
  }

  @Override
  public int getUIDForUser(Configuration conf, String user, int defaultUID)
      throws Exception {
    return Integer.parseInt(Shell.execCommand("id -u").trim());
  }

  @Override
  public String getGroupForGID(Configuration conf, int gid, String defaultGroup)
      throws Exception {
    return getCurrentGroup();
  }

  @Override
  public String getUserForUID(Configuration conf, int gid, String defaultUser)
      throws Exception {
    return getCurrentUser();
  }
  public static String getCurrentGroup() {
    File script = null;
    try {
      script = File.createTempFile("group", ".sh");
      String[] cmd = {"bash", script.getAbsolutePath()};
      Files.write("groups | cut -d ' ' -f 1", script, Charsets.UTF_8);
      return Shell.execCommand(cmd).trim();
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      if(script != null) {
        script.delete();
      }
    }
  }
}
