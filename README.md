# How to use

1. Requirements:

        - HDFS Instance with CDH, Apache Hadoop 0.23+, or Apache Hadoop 1.0+
        - Note that this has only been tested extensive on RHEL 
          (or equivalent) with 5.7 and 6.X as NFS Clients with CDH3u2.

1. Create the mount location

        $ sudo mkdir /mnt/hdfs

1. Add this entry to /etc/fstab

        localhost:/   /mnt/hdfs   nfs4       rw,intr,timeo=600,proto=tcp      0 0

1. Ensure you have maven installed and hadoop command configured with *-site.xml pointing at the namenode.

1. Build the package which includes dependencies. You need choose the appropiate hadoop version when
building the package. Examples below:

        0.20 Branch:
         CDH3u2:
          $ mvn package -Phadoop-0.20 -Dhadoop.version=0.20.2-cdh3u2
         Apache Hadoop 1.0.0 (Renamed 0.20):
          $ mvn package -Phadoop-0.20 -Dhadoop.version=1.0.0

        0.23 Branch:
         $ mvn package -Phadoop-0.23 -Dhadoop.version=0.23.1-SNAPSHOT

1. Start the server:

        $ ./start-nfs-server.sh

1. Mount hdfs

        $ sudo mount /mnt/hdfs

1. You should now be able to access HDFS. Note: The script ./start-nfs-client-tests.sh runs basic tests.

# What needs improvement (in no order)

* Locking:
NFS4Handler has coarse grained locking and is heavily used
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
* Write Ordering:
We buffer writes until we find the prereq, this memory consumption is not bounded.
* Metrics:
A simple metrics system is used. We should use Hadoops Metric System. 
* Damn Ugly:
 
         - DirectoryEntry.getWireSize is sick and wrong
         - Bitmap

# FAQ

* I am running an NFS Server on port 2049, how can I configure this to use another port?

         1) Change 2049 in start-nfs-server.sh to say 2050
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
