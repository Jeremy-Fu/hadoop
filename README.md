#Building A MapReduce Facility
==============================

##1. Objective

In this project, we are implementing a MapReduce facility like Hadoop which can process data in parallel by exploiting the independency of data.

##2. System Design

Our MapReduce framework consists of two major parts: (1) distributed file system(DFS) and (2) MapReduce framework. Our DFS provides a global access to file where several replicas are stored in different nodes. And the MapReduce framework divides the data into chunks and processes each of chunks in parallel to achieve high performance.

###2.1 Distributed File System(DFS)

####General Description

Our DFS consists of two major parts: (1) NameNode and (2) DataNode. NameNode manages files metadata, periodically checks the status of DataNodes and files. DataNode is where data is stored and reports its overall availability to NameNode.

Each file in DFS is denoted as an HDFSFile whose metadata is managed by NameNode. Each HDFSFile contains file name, a list of chunks into which the file is divided, replica factor that describes the enforced replication amount for each file, and overall availability of a file. 

HDFSChunk is the abstraction of chunks. Each HDFSChunk keeps the chunk name and the locations for each of replicas. The chunk name is the name recognized by DataNode's file system. HDFSChunk's chunksize is configurable.

The DFS is customized for our MapReduce and random access is not supported. Unlike Hadoop Distirbuted File System(HDFS) which chops large data into equal size chunks, we divides a file into chunks with end of a new line feed with an exception in last chunk. As our MapReduce framework is customized for processing data line by line, each chunk is divided by a new line feed so that each chunk can be fed to a Mapper task at a time and no more further data traffic is needed between different chunks during the MapReduce. This customization leaves the little difference of length among chunks but provides a decent interface to MapReduce.

####Semantics

The way the file uploaded to distributed file systems is as follows: The client request to create a file. If the file name has been used, a failure is returned back to client. After client successfully requested a file discriptor, the client is in charge of maintaining HDFSFile objcet. The HDFSFile object contains all information of this file, such as the file name, metadata of all chunks. The HDFSOutputStream is implicitly created by getOutputStream() method instead of the constructor of HDFSOutputStream. With HDFSOutputStream, the client can feed data to the distributed system. As client continues writing from local to the system, the DFS won’t see the any updates of this file before the output stream is closed. Therefore, despite the client has already written some chunks to the file systems, the file is invisible. Once the client successfully closes the output stream, the file is visible at client. 

In our distributed system, if two different HDFSOutputStream objects write data seperately to the DFS, the file well be overlapped by file. That is to say, the version of the file depends the latest close operation. 

Our distributed file systems are IP-aware, by which we believe that the smaller difference of two IPs represents the closer geographic location. By utilizing this feature, the MapReduce framework is able to process work as close to the data node as possible. As moving data is much more expensive than moving process, this helps to improve the MapReduce framework a lot. And even if the data needs to be carried around,  the traffic is well limited to a small networks because the distributed file system is IP-aware. During reading the file, the HDFSInputStream tries to find the nearest valid DataNode to retrieve chunks.

####NameNode

The NameNode is the coordinator of the DFS. It doesn’t deal with any I/O manipulation. Instead, it monitors the status of each DataNode, the file availability and enforces the robustness of each files. As it is the coordinator of the distributed file systems, only metadata of each file, chunk and DataNode is stored by NameNode. 

NameNode acts as the only naming service within the the system. As a administrative node, NameNode contains a lot of relationship information of chunks. For example, the NameNode should know the map from file name to file metadata. Besides, the NameNode should also know the metadata of each chunk of each file. It is in charge of naming each of chunk as well. The synchronized naming service is provided at NameNode only with help of timestamp and provides a decent “happens before” relationship among files.

By design, NameNode and DataNode communicates in one way via heartbeat. DataNode periodic sends heartbeat to NameNode to show its existence, NameNode passes tasks back to DataNode. This simplifies the communication complexity and also effectively prevents the dead locks.

In order to maintain the health of distributed file systems, the NameNode should do a periodic system check. This periodic system check collects the chunks information generated by each DataNode and compare it with the metadata kept by itself. 3 different situations are handled by NameNode: (1). the chunks which is detected by DataNodes while NameNode is unaware of its existence are called orphans. (2) Chunks that are less than the specified replica factor need to be replicated in time to improve the robustness of the total systems. (3) Chunks that are over replicated are enforced to be deleted in order to save spaces on each DataNode.

Load balance is handled by our distributed systems as well. NameNode selects the DataNodes with lightest chunks number upon clients request the destination to write data to. Even though the data is not precisely equal in length, they are regarded as the same by our system because the difference between by different chunks mostly varies tens of bytes to hundreds of bytes which is ignorable compared to each chunk.

####DataNode

The DataNode is known as storage node in our systems. DataNode stores chunks without the knowledge of which files they belong to. By design, the DataNode always initiates the communication and informing its status and take action as NameNode requests. Besides communicating with NameNode with its status, it receives task from NameNode and perform tasks. DataNode also responds to file I/O request from clients. 

DataNode often initiates two types communication with NameNode: (1) HeartBeat and (2) Chunk Report. The heartbeat informs NameNode the existence of DataNode. The chunk report sends the chunks information on the node to NameNode to enable NameNode monitoring the status of each file. Since the chunk report may be huge as the accumulative chunks, the chunk report is much less frequent than heartbeat. Between two chunk reports, NameNode keeps track of manipulation on each DataNode. Upon the chunk reports, the NameNode update the information with the help of chunk report as chunks report reflects more real-time information of each chunk. As illustrated in NameNode section, the DataNode may delete orphans, make replications and delete redundant replications upon NameNode’s request.

DataNode also exposes several remote method invocations to clients such as request chunks of files. Those invocations contain write to or read from chunks. DataNode has relatively lower computational task but a heavier I/O manipulations.

####Client

Client is a machine requesting distributed file system service. Our distributed file systems provides 4 basic service to clients: (1) create a file and write to lightest-load node. (2) open a file and read from it from closet DataNode (3) remove a file on NameNode and corresponding chunks on DataNode. (4) list all files on the systems.

![alt tag](https://raw.githubusercontent.com/Jeremy-Fu/hadoop/master/fig1.png)

To conclude, the above figure shows the general traffic between different components. NameNode takes the most heavy computation tasks. But it doesn’t take a I/O traffic burden. DataNode is in charge of I/O with client.

####Fault Tolerance

The DFS suffers from the one-point-failure. If the NameNode is partitioned by network or fails down, all services are declined.

Due to the unreliable nature of Internet, we spent efforts in dealing with network partitions. if a partition happens several results will occur:

1. Opening and creating file succeed only if the client is in the same partition as NameNode.
2. Read succeeds if at least one DataNode is in the same partition with DataNode, and NameNode doesn’t have to be in the same partition because the chunk information has all been included in HDFSFile.
3. Write may succeed if there is at least one available DataNode and the NameNode has to be in the same partition as well. We say write may succeed because it depends the destination DataNode assigned by NameNode. And NameNode may not notice the partition until a system check starts and the partition time(estimated by the difference between current time and last recent heartbeat) exceeds the partition toleration. 

If the DataNode doesn’t send heartbeat to NameNode, the NameNode believes it to be unavailable. Once the DataNode is believed to be unavailable, NameNode won’t count on chunks information reported from such DataNode. And most of time, several actions are taken subsequently. For example new replications are often made on available nodes. In extreme situation, all replicas of a chunk is missed, the file is believed to be unavailable.

###2.2 MapReduce Framework

####General Description

The distirbuted computing MapReduce framework resides on DFS. MapReduce leverages the characteristic of location-awareness of DFS to allocate computation on nodes where data resides so that the network traffic to move data doesn't become the bottle neck to limit the computation performance. There are three major components in our MapReduce framework: JobClient, JobTracker and TaskTracker. 

####JobClient

JobClient provides a bridge for MapReduce application to submit their jobs to JobTracker within the MapReduce cluster. It takes the responsibility to receive a job submission from application with configurations, sends submission message to JobTracker to submit the job and reports the current progress of the job periodically.

####JobTracker

JobTracker plays as a central coordinator in our MapReduce system, it manages the access to the computational resources on all TaskTrackers. All communications between JobTracker and other components(TaskTracker and JobClient) are through Remote Method Invocation. Every TaskTracker sends a message to the JobTracker within a cluster to join the MapReduce cluster, with its IP address and the number of available slots to run tasks on it. The JobTracker receives join message from TaskTracker and maintains a status record of each TaskTracker. 

For each job, the basic working flow on a JobTracker is described as follows: upon receiving a job submission from JobClient, JobTracker initiates several mapper tasks according to those chunks that make up the input file. It generally creates one mapper task per chunk and pushes those tasks to JobScheduler to do scheduling work for each task. Once all mapper tasks of this job are report as SUCCESS from TaskTrackers, the JobTracker initiates corresponding reducer tasks and pushes to JobScheduler. Upon SUCCESS of all reducer tasks, the status of the job will be marked as SUCCESS and not until then will the JobTracker inform associative TaskTrackers to clean up all those intermediate files produced during mapper and reducer phases.  

JobScheduler is a part of JobTracker. It maintains a task queue for each registered TaskTrackers, from which TaskTracker can fetch their new assignments for the next round. When a mapper task is pushed to JobScheduler, it first checks the current workload of those TaskTrackers that have the chunk of data for this mapper task locally and tries to pick one TaskTracker with lightest workload. If all those locality-optimal TaskTrackers are currently overload, the JobScheduler then goes through all TaskTrackers and picks the one with lightest workload. With this scheduling schema, our MapReduce cluster are supposed to gain a trade-off between TaskTrackers that are either with chunk data locality or shorter queueing time for a mapper task. And this schema can be adjust by modify the overload condition judgement in scheduler’s function, so as to gain better performance under different circumstances. The scheduling schema for each TaskTracker’s task queue of our JobScheduler is a combination of FIFO and the priority factor of different task: if the job is submitted with no priority level configured, all those tasks of this job are created with priority level 0, which is the basic level. Tasks with higher priority level are always at the head of the queue after being inserted. For tasks with the same priority, their positions in one queue are decided by their job IDs(earlier job’s tasks go first) and then task IDs(earlier task IDs go first). 

Every TaskTracker heartbeats the JobTracker by calling the JobTracker’s heartbeat method every second. TaskTrackers communicate with JobTracker by reporting current status of all tasks on it. The JobTracker updates associative task status, tries re-schedule if there are FAILED tasks and sends back acknowledgements to those tasks that are reported as SUCCESS and FAILED, so that the TaskTracker will not report their status in future.

JobTracker also plays an important role in providing fault tolerance for MapReduce framework. There are basically two general failures among out MapReduce clusters: task failure and TaskTracker failure. 

For mapper tasks that are reported as FAILED from TaskTrackers, these tasks will be re-scheduled up to MAX_RESCHEDULE_ATTEMPTS times, each time it is being re-scheduled by JobScheduler (JobScheduler will not schedule this task to the TaskTracker that failed this taks), theirs priority level will be upgraded by 1 so that the FAILED mapper tasks gain higher priority over other normal tasks and can be executed by TaskTracker soon. Upon a FAILED reducer task is reported by TaskTracker through heartbeat, the JobTracker will reset the job to its initial state: all mapper tasks of this job will be re-scheduled by JobScheduler and then reducer tasks again. The maximum number of attempts to re-schedule a job can also be configured by MAX_RESCHEDULE_ATTEMPTS.

The failure of a TaskTracker is handled by a daemon thread running on JobTracker. Since TaskTracker periodically reports all the states of tasks currently on it through heatbeat with JobTracker. JobTracker updates a TaskTracker’s time stamp upon receiving heartbeat message. The TaskTrackerCheck thread periodically checks the time stamp of each TaskTracker, once it finds one of which the time stamp expires the TASKTRACKER-EXPIRATION,  it notifes the JobTrakcer to disable the TasktRacker, which will not be enabled again until a heartbeat from this TaskTracker. The JobScheduler then tries to: a. re-schedule those tasks currently on this TaskTracker’s queue to other places; b. check the FAILED TaskTracker’s history to ensure there isn’t any tasks complete on it are further needed by other tasks, otherwise those necessary tasks should be re-schedule(e.g. another reducer task may need those intermediate files out of a specific mapper task on the FAILED TaskTracker).

####TaskTracker

TaskTracker is a daemon waiting for task assigned from JobTracker. By design, TaskTracker and JobTracker can only communicate with heartbeat. Via heartbeating, TaskTracker receives tasks assigned by JobTracker. TaskTracker also updates JobTracker with each task status. There are in total 4 kinds of tasks: MapperTask, ReducerTask, KillerTask, CleanerTask. On the other hand, there are another thread periodically updating the status of each tasks. 
In order to limit the traffic to reasonable bytes, if task terminates successfully or with error, the JobTracker is notified by TaskTracker with terminated tasks and finally JobTracker acknowledged TaskTracker, the task is no longer tracked by TaskTracker. Therefore, the tasks status to be transferred are those running at the sending time and haven’t been acknowledged by JobTracker.

Upon arrival of a Mapper or Reducer task, TaskTracker starts a new  JVM to launch the Mapper or Reducer task. The idea to start a new process instead of thread is to ensure the total partitioned memory from client’s MapReduce job and achieve the internal stability of our TaskTracker. The following will describe four tasks in details.

Besides manage the tasks assigned by JobTracker, TaskTracker also provides downloading service for shuffling. In the design of our system, we faced with two options to provide such service that is either by socket or RMI. We chose to use socket communication to shuffle because RMI draws some limitation on transferring files. For example it is costly retrieve large intermediate file at once. And if we want to retrieve the file via multiple times, a transactional I/O is required. And RMI also imposes some overhead. At last we decided to use socket to transfer files.

#####Run Mapper

MapperTask intrigues a new JVM for Mapper. Before launch the Mapper, the TaskTracker first download the .jar file from HDFS so that the client’s Mapper and Reducer class can be launched. The task tracker gets the chunk to be processed by Mapper, which is assigned by JobTracker based on location of data. In most of time, the Mapper is executed where data resides. Therefore the latency to retrieve data is minimized. Besides, the traffic is also restricted within host. The OutputCollector is charge of collect the output of Mapper. Since the size of outputs of Mapper is unpredictable and can end up with very large, an external sort is adopted. As a result, the OutputCollector collects a bunch of outputs whose size is configurable (??really) at configuration, sorts the key value pair and dump them to disks. Iteratively, it generates a couple of intermediate files which is sorted in ascending order in each file. Finally, OutputCollector merges those files and write to corresponding partition files.

#####Run Reducer

ReducerTask intrigues a new JVM for Reducer. Before launch the Reducer, the TaskTracker checks the unavailability of .jar file as well. In the preparation, Reducer needs to collect files from different Mappers which is know as shuffling. In our implementation, if a file is located locally, a rename action is taken instead of socket downloading. Similary, the RecordReconstructor and OutputCollect perform external sort as well.


The task tracker gets the chunk to be processed by Mapper, which is assigned by JobTracker based on location of data. In most of time, the Mapper is executed where data resides. Therefore the latency to retrieve data is minimized. Besides, the traffic is also restricted within host. The OutputCollector is charge of collect the output of Mapper. Since the size of outputs of Mapper is unpredictable and can end up with very large, an external sort is adopted. As a result, the OutputCollector collects a bunch of outputs whose size is configurable (??really) at configuration, sorts the key value pair and dump them to disks. Iteratively, it generates a couple of intermediate files which is sorted in ascending order in each file. Finally, OutputCollector merges those files and write to corresponding partition files.
