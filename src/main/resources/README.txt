Questions? email brock at cloudera dot com

# How to use

1. Requirements:

        - HDFS Instance with CDH, Apache Hadoop 2.x+, or Apache Hadoop 1.x+
        - NFS Packages (nfs-utils on RHEL, CentOS and nfs-common on Ubuntu)
          must be installed on any clients wishing to mount HDFS
        - A native NFS Server cannot be running the hosts unless you change
          the port of HDFS NFS Proxy, instructions in the FAQ
        
1. Download

     Either clone via git or download a zip: https://github.com/brockn/hdfs-nfs-proxy/zipball/master

1. Create the mount location

        $ sudo mkdir /mnt/hdfs

1. Add this entry to /etc/fstab

        <hostname of server running the proxy>:/   /mnt/hdfs   nfs4       rw,intr,timeo=600      0 0

1. Choose binary:

    Either build a binary or choose one of the pre-built snapshots. The snapshots are in the snapshots/ directory.

    Should you choose to build your own binary, you need choose the appropiate hadoop version when building the package. Examples below:

        Hadoop 1:
          $ mvn package -Pcdh3
          $ mvn package -Phadoop1

        Hadoop 2:
          $ mvn package -Pcdh4
          $ mvn package -Phadoop2

    The output jar will be target/ you likely want the -with-deps jar.

1. Start the server:

        Below, /usr/lib/hadoop/conf, is the path to my *-site.xml files. Note that
        the daemon should be started as the user running hdfs, typically hadoop or hdfs.

        To run the server on port 2049:

        $ ./start-nfs-server.sh /usr/lib/hadoop/conf 2049

1. Mount hdfs

        $ sudo mount /mnt/hdfs

1. You should now be able to access HDFS via ls -la /mnt/hdfs 

# FAQ

* I am running an NFS Server on port 2049, how can I configure this to use another port, say 2050?

         1) Change your start command to: ./start-nfs-server.sh /usr/lib/hadoop/conf 2050
         2) Add port=2050 to the mount options

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

* I am trying to do an operation as any user who is not running the daemon and
I get errors?

You will only be able to access HDFS as the user running the daemon unless
you run the daemon as the user running the namenode (e.g. hadoop or hdfs)
or have Secure Impersonation configured for the user running the proxy
(http://hadoop.apache.org/common/docs/current/Secure_Impersonation.html).

Say I have the proxy running as noland and I copy a file as root into
/user/root, I will get this error below:

    org.apache.hadoop.security.AccessControlException: Permission denied: 
      user=noland, access=WRITE, inode="/user/root":root:hadoop:drwxr-xr-x

The easiest option is to start the daemon as hadoop, hdfs or whatever user
is running your namenode.

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
         - Kerberos
         - File appends

* Read Ordering: 
We recieve a fair number of threads blocked on reads of a single input stream.
I think we could get better performance if we ordered these like writes because we
know they will arrive out of order. As such the current impl is doing more seeks
than required. We also might considering pooling input streams.
* Garbage Collection:
Heavy read loads use a fair amount of old gen likely due to our response cache
and the size of read responses. If this becomes an issue we could easily exclude 
read requests from the response cache.
* Metrics:
A simple metrics system is used. We should use Hadoops Metric System. 
