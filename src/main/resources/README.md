Questions? email brock at cloudera dot com

# How to use

1. Requirements:

    # HDFS Instance of CDH3 or CDH4
    # NFS Packages (nfs-utils on RHEL, CentOS and nfs-common on Ubuntu)
    must be installed on any clients wishing to mount HDFS
        
1. Download

     Either clone via git or download a zip: https://github.com/brockn/hdfs-nfs-proxy/zipball/master

1. Create the mount location

        $ sudo mkdir /mnt/hdfs

1. Add this entry to /etc/fstab

        <proxy hostname>:/   /mnt/hdfs   nfs4       rw,intr,timeo=600,port=2501      0 0

1. Build binary:

   Install JDBM in your local repo:
         $ mvn install:install-file -Dfile=lib/jdbm-2.4.jar -DgroupId=thirdparty \
           -DartifactId=jdbm -Dversion=2.4 -Dpackaging=jar

    Choose the appropiate hadoop version when building the package. Examples below:

    Hadoop 1:
          $ mvn package -Pcdh3
          $ mvn package -Phadoop1

    Hadoop 2:
          $ mvn package -Pcdh4
          $ mvn package -Phadoop2

    The output jar will be target/ you likely want the -with-deps jar.

1. Start the server:

    Below my conf/ directory has an hdfs-nfs-site.xml, hdfs-site.xml, core-site.xml,
    and a log4j.properties file. Note the daemon would typically be started as the 
    same user who is running the NameNode typically either hdfs or hadoop.

        To run the server on port 2051:

        $ ./start-nfs-server.sh confi/ 2051

1. Mount hdfs

        $ sudo mount -v /mnt/hdfs

1. You should now be able to access HDFS via ls -la /mnt/hdfs 

# FAQ

* How do I use the dameon with Kerberos security enabled?

Note this support is experimental and requires all users must have kerberos principals.

    # Have a working Keberos enabled Hadoop cluster
    # Generated a keytab for the nfs/_HOST@DOMAIN user
    # Enable security via the options described in hdfs-nfs-site.secure-sample.xml
    # Enable sec=krb5p on the client mount

* How can I configure this to use another port, say 2051?

    # Change your start command to: ./start-nfs-server.sh conf/ 2055
    # Add port=2055 to the mount options

* What is this good for? or Can I replace my expensive NAS?

This is another way to access HDFS. It is not a replacement 
for NAS when you need the functionality NAS provides. To be sure,
this does not provide you the ability to run Oracle over NFS on 
top of HDFS.

* All user/groups show up as nobody?

NFS4 returns user/group with user@domain. Today by default it responds with
user@clientDomain. The client then uses the idmap service to lookup the user
for a uid. As such, it's likely you have not configured idmap.

Say the domain is acme.com, you would change: /etc/idmapd.conf from:

        Domain = localdomain

to:

        Domain = bashkew.com

and then restart idmapd:
 
       /etc/init.d/rpcidmapd restart

* All user/groups still show up as nobody or some long number?

Disable ipv6

# What needs improvement (in no order)

None of these make the system unusable, they are just things we should improve.

* User Mapping: 
NFS4 User identities are user@domain. However, the RPC protocol uses UID/GID.
Currently we map the UID on the incoming request via the system the daemon executes on.
I think there is something in Hadoop which does user mapping as well. If so, it might
make sense to be consistent.
* RFC 3530 (NFS4):

         - Client ID logic is complex and not completely followed.
         - Many reccomended attributes are not implemented such as 14 archive, 25 hidden,
         49 timebackup, 55 mounted on fileid
         - File appends
* Metrics:
A simple metrics system is used. We should use Hadoops Metric System. 
