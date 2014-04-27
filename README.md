# Hadoop Fair Sojourn Protocol (HFSP)

The Hadoop Fair Sojourn Protocol Scheduler is a size-based scheduler for
Hadoop. A presentation of the scheduler can be found [here](http://www.slideshare.net/melrief/main-34002097).

## Compile HFSP

In order to compile HFSP you need [Maven](http://maven.apache.org/). From
the top directory issue the following command:

```
$ mvn package -Dmaven.test.skip=true
```

This will create two files in the directory _/target_:
- _hfsp-scheduler-1.0-cdh4.4.0.jar_: a jar file containing the scheduler
- _hfsp-scheduler.xml_: a default configuration file

## Use HFSP

Copy _hfsp-scheduler-1.0-cdh4.4.0.jar_ in your Hadoop directory. Optionally, add the
configuration file for HFSP in the hadoop configuration directory.

Set HFSP as task scheduler in conf/mapred-site.xml:

```xml

<configuration>
	<property>
          <name>mapred.jobtracker.taskScheduler</name>        
          <value>org.apache.hadoop.mapred.HFSPScheduler</value>  
	</property>
</configuration>
```

## Hadoop versions

HFSP has been developed for the current stable version of Hadoop 1.x, that is 
Hadoop 1.1.2.

## Papers

- [HFSP: Size-based scheduling for Hadoop](http://ieeexplore.ieee.org/xpls/abs_all.jsp?arnumber=6691554&tag=1)
- [ Revisiting Size-Based Scheduling with Estimated Job Sizes](http://arxiv.org/abs/1403.5996)

## Contributors

- Mario Pastorelli (pastorelli.mario@gmail.com)
- Antonio Barbuzzi (antoniob82@gmail.com)

## Acknowledgements

The HFSP project is part of the [BigFoot project](http://www.bigfootproject.eu/)
