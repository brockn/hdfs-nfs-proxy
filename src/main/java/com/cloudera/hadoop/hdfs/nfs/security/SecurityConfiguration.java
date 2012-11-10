package com.cloudera.hadoop.hdfs.nfs.security;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import com.cloudera.hadoop.hdfs.nfs.PathUtils;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class SecurityConfiguration {

  private final Configuration mConfiguration;
  public SecurityConfiguration(Configuration conf) {
    mConfiguration = conf;
  }
  
  public void configure() throws IOException {
    if(SECURITY_FLAVOR_KERBEROS.equalsIgnoreCase(mConfiguration.get(SECURITY_FLAVOR))) {
      String realm = getRequired(SECURITY_KERBEROS5_REALM);
      String kdc = getRequired(SECURITY_KERBEROS5_KDC);
      String keyTab = getRequired(SECURITY_KERBEROS5_KEYTAB);
      String principal = getRequired(SECURITY_KERBEROS5_PRINCIPAL);
      final File baseDir = Files.createTempDir();
      Runtime.getRuntime().addShutdownHook(new Thread() {
        public void run() {
          PathUtils.fullyDelete(baseDir);
        }
      });
      File loginConf = new File(baseDir, "sec.conf");
      Files.write(getLoginFileContents(keyTab, principal), loginConf, Charsets.UTF_8);
      System.setProperty("java.security.krb5.realm", realm);
      System.setProperty("java.security.krb5.kdc", kdc);
      System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
      System.setProperty("java.security.auth.login.config", loginConf.getAbsolutePath());
      UserGroupInformation.loginUserFromKeytab(principal, keyTab);
    }
  }
  
  private String getRequired(String key) {
    String value = mConfiguration.get(key);
    if(value == null) {
      throw new IllegalArgumentException("Require property not found " + key);
    }
    return value;
  }
  
  private String getLoginFileContents(String keytab, String principal) {
    String contents =
    "com.sun.security.jgss.initiate {\n" +
      "\tcom.sun.security.auth.module.Krb5LoginModule required;\n" +
    "};\n" +
    "com.sun.security.jgss.accept {\n" +
      "\tcom.sun.security.auth.module.Krb5LoginModule required\n" +
        "\tuseKeyTab=true\n" +
        "\tstoreKey=true\n" +
        "\tdoNotPrompt=true\n" +
        "\tkeyTab=\""+keytab+"\"\n" +
        "\tprincipal=\""+ principal + "\";\n" +
    "};\n";
    return contents;
  }
}
