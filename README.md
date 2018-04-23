**This project will be proactively monitored and updated**

# jraft
Raft consensus implementation in java

The core algorithm is implemented based on the TLA+ spec, whose safety is proven, and liveness is highly depended on the pseudo random number sequence, which could be fine if different servers in the same cluster are generating random numbers with different seeds.

## Supported Features,
- [x] Core Algorithm, safety is proven
- [x] Configuration Change Support, add or remove servers one by one without limitation
- [x] Client Request Support
- [x] **Urgent commit**, see below
- [x] log compaction 

> Urgent Commit, is a new feature introduced by this implementation, which enables the leader asks all other servers to commit one or more logs if commit index is advanced. With Urgent Commit, the system's performance is highly improved and the heartbeat interval could be increased to seconds,depends on how long your application can abide when a leader goes down, usually, one or two seconds is fine. 

## About this implementation
it's always safer to implement such kind of algorithm based on Math description other than natural languge description.
there should be an auto conversion from TLA+ to programming languages, even they are talking things in different ways, but they are identical

> In the example of dmprinter (Distributed Message Printer), it takes about 4ms to commit a message, while in Active-Active scenario (sending messages to all three instances of dmprinter), it takes about 9ms to commit a message, the data is collected by CLT (Central Limitation Theory) with 95% of confidence level.

## Code Structure
This project contains not that much code, as it's well abstracted, here is the project structure
* **core**, the core algorithm implementation, you can go only with this, however, you need to implement the following interfaces,
  1. **Logger** and **LoggerFactory**
  2. **RpcClient** and **RpcClientFactory**
  3. **RpcListener**
  4. **ServerStateManager** and **SequentialLogStore**
  5. **StateMachine**
* **exts**, some implementations for the interfaces mentioned above, it provides TCP based CompletableFuture<T> enabled RPC client and server as well as **FileBasedSequentialLogStore**, with this, you will be able to implement your own system by only implement **StateMachine** interface
* **dmprinter**, a sample application, as it's name, it's distributed message printer, for sample and testing.
* **setup**, some scripts for Windows(R) platform to run **dmprinter**

## Run dmprinter
dmprinter is a distributed message printer which is used to verify the core implementation. A set of Windows(R) based setup scripts is shipped with this project, under **setup** folder.
  1. Export three projects into  one executable jar file, called jraft.jar
  2. start a command prompt, change directory to **setup** folder
  3. run **setup.cmd**, it will start three instances of jraft
  4. write a simple client by using python or whatever language you would love to to connect to any port between 8001 to 8003, which dmprinter is listening on. **message format** see below
  5. you can call addsrv.cmd \<server-id-in-int\> to start new instance of raft and call addsrv:\<server-id-in-int\>,tcp://localhost:900\<server-id-in-int\> to join the server to cluster, e.g. **addsrv:4,tcp://localhost:9004** through the client, or remove a server from cluster **rmsrv,4**, check out more details about the message format as below,
  
> Message format \<header\>\<message\>

> \<header\> := four bytes, which is encoded from an integer in little-endian format, the integer is the bytes of the \<message\>

> \<message\> := \<id|command\>:\<content\>

> \<id\> := uuid

> \<command\> := addsrv|rmsrv

> \<content\> := srvid,uri|srvid|any-string
