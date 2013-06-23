# Hadoop Fair Sojourn Protocol (HFSP)

The Hadoop Fair Sojourn Protocol Scheduler is a size-based scheduler for
Hadoop.

## Compile HFSP

In order to compile HFSP you need [Maven](http://maven.apache.org/). From
the top directory issue the following command:

```
$ mvn package
```

This will create two files in the directory _/target_:
- _hfsp-scheduler-1.0.jar_: a jar file containing the scheduler
- _hfsp-conf.xml_: a default configuration file

## Use HFSP

Copy _hfsp-scheduler-1.0.jar_ in your Hadoop directory. Optionally, add the
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

## Acknowledgements

The HFSP project is part of the [BigFoot project](http://www.bigfootproject.eu/)
