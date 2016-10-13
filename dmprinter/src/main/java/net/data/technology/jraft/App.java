/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  The ASF licenses 
 * this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.data.technology.jraft;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import net.data.technology.jraft.extensions.FileBasedServerStateManager;
import net.data.technology.jraft.extensions.Log4jLoggerFactory;
import net.data.technology.jraft.extensions.RpcTcpClientFactory;
import net.data.technology.jraft.extensions.RpcTcpListener;

public class App
{
    public static void main( String[] args ) throws Exception
    {
        if(args.length < 2){
            System.out.println("Please specify execution mode and a base directory for this instance.");
            return;
        }

        if(!"server".equalsIgnoreCase(args[0]) && !"client".equalsIgnoreCase(args[0]) && !"dummy".equalsIgnoreCase(args[0])){
            System.out.println("only client and server modes are supported");
            return;
        }

        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(Runtime.getRuntime().availableProcessors() * 2);
        if("dummy".equalsIgnoreCase(args[0])){
            executeInDummyMode(args[1], executor);
            return;
        }

        Path baseDir = Paths.get(args[1]);
        if(!Files.isDirectory(baseDir)){
            System.out.printf("%s does not exist as a directory\n", args[1]);
            return;
        }

        FileBasedServerStateManager stateManager = new FileBasedServerStateManager(args[1]);
        ClusterConfiguration config = stateManager.loadClusterConfiguration();

        if("client".equalsIgnoreCase(args[0])){
            executeAsClient(config, executor);
            return;
        }

        // Server mode
        int port = 8000;
        if(args.length >= 3){
            port = Integer.parseInt(args[2]);
        }
        URI localEndpoint = new URI(config.getServer(stateManager.getServerId()).getEndpoint());
        RaftParameters raftParameters = new RaftParameters()
                .withElectionTimeoutUpper(5000)
                .withElectionTimeoutLower(3000)
                .withHeartbeatInterval(1500)
                .withRpcFailureBackoff(500)
                .withMaximumAppendingSize(200)
                .withLogSyncBatchSize(5)
                .withLogSyncStoppingGap(5)
                .withSnapshotEnabled(5000)
                .withSyncSnapshotBlockSize(0);
        MessagePrinter mp = new MessagePrinter(baseDir, port);
        RaftContext context = new RaftContext(
                stateManager,
                mp,
                raftParameters,
                new RpcTcpListener(localEndpoint.getPort(), executor),
                new Log4jLoggerFactory(),
                new RpcTcpClientFactory(executor),
                executor);
        RaftConsensus.run(context);
        System.out.println( "Press Enter to exit." );
        System.in.read();
        mp.stop();
    }

    private static void executeAsClient(ClusterConfiguration configuration, ExecutorService executor) throws Exception{
        RaftClient client = new RaftClient(new RpcTcpClientFactory(executor), configuration, new Log4jLoggerFactory());
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while(true){
            System.out.print("Message:");
            String message = reader.readLine();
            if(message.startsWith("addsrv")){
                StringTokenizer tokenizer = new StringTokenizer(message, ";");
                ArrayList<String> values = new ArrayList<String>();
                while(tokenizer.hasMoreTokens()){
                    values.add(tokenizer.nextToken());
                }

                if(values.size() == 3){
                    ClusterServer server = new ClusterServer();
                    server.setEndpoint(values.get(2));
                    server.setId(Integer.parseInt(values.get(1)));
                    boolean accepted = client.addServer(server).get();
                    System.out.println("Accepted: " + String.valueOf(accepted));
                    continue;
                }
            }else if(message.startsWith("fmt:")){
                String format = message.substring(4);
                System.out.print("How many?");
                String countValue = reader.readLine();
                int count = Integer.parseInt(countValue.trim());
                for(int i = 1; i <= count; ++i){
                    String msg = String.format(format, i);
                    boolean accepted = client.appendEntries(new byte[][]{ msg.getBytes() }).get();
                    System.out.println("Accepted: " + String.valueOf(accepted));
                }
                continue;
            }else if(message.startsWith("rmsrv:")){
                String text = message.substring(6);
                int serverId = Integer.parseInt(text.trim());
                boolean accepted = client.removeServer(serverId).get();
                System.out.println("Accepted: " + String.valueOf(accepted));
                continue;
            }

            boolean accepted = client.appendEntries(new byte[][]{ message.getBytes() }).get();
            System.out.println("Accepted: " + String.valueOf(accepted));
        }
    }

    /**
     * This is used to verify the rpc module's functionality
     * @param mode
     */
    private static void executeInDummyMode(String mode, ExecutorService executor) throws Exception{
        if("server".equalsIgnoreCase(mode)){
            RpcTcpListener listener = new RpcTcpListener(9001, executor);
            listener.startListening(new DummyMessageHandler());
            System.in.read();
        }else{
            RpcClient client = new RpcTcpClientFactory(executor).createRpcClient("tcp://localhost:9001");
            int batchSize = 1000;
            List<Pair<RaftRequestMessage, CompletableFuture<RaftResponseMessage> > > list = new LinkedList<Pair<RaftRequestMessage, CompletableFuture<RaftResponseMessage> > >();
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            System.out.print("ready to start?");
            reader.readLine();
            while(true){
                for(int i = 0; i < batchSize;  ++i){
                    RaftRequestMessage request = randomRequest();
                    request.setSource(i);
                    CompletableFuture<RaftResponseMessage> response = client.send(request);
                    list.add(new Pair<RaftRequestMessage, CompletableFuture<RaftResponseMessage>>(request, response));
                }

                for(int i = 0; i < batchSize; ++i){
                    System.out.printf("Waiting for response %d\n", i);
                    Pair<RaftRequestMessage, CompletableFuture<RaftResponseMessage> > item = list.get(i);
                    RaftRequestMessage request = item.item1;
                    RaftResponseMessage response = item.item2.get();

                    System.out.println(String.format(
                            "Response %d: Accepted: %s, Src: %d, Dest: %d, MT: %s, NI: %d, T: %d",
                                i,
                                String.valueOf(response.isAccepted()),
                                response.getSource(),
                                response.getDestination(),
                                response.getMessageType(),
                                response.getNextIndex(),
                                response.getTerm()));

                    if(request.getTerm() != response.getTerm()){
                        System.out.printf("fatal: request and response are mismatched, %d v.s. %d @ %s!\n", request.getTerm(), response.getTerm(), item.item2.toString());
                        reader.readLine();
                        return;
                    }
                }

                System.out.print("Continue?");
                String answer = reader.readLine();
                if(!"yes".equalsIgnoreCase(answer)){
                    break;
                }

                list.clear();
            }
        }
    }

    private static Random random = new Random(Calendar.getInstance().getTimeInMillis());

    private static RaftRequestMessage randomRequest(){
        RaftRequestMessage request = new RaftRequestMessage();
        request.setMessageType(randomMessageType());;
        request.setCommitIndex(random.nextLong());
        request.setDestination(random.nextInt());
        request.setLastLogIndex(random.nextLong());
        request.setLastLogTerm(random.nextLong());
        request.setSource(random.nextInt());
        request.setTerm(random.nextLong());
        LogEntry[] entries = new LogEntry[random.nextInt(20) + 1];
        for(int i = 0; i < entries.length; ++i){
            entries[i] = randomLogEntry();
        }

        request.setLogEntries(entries);
        return request;
    }

    private static RaftMessageType randomMessageType(){
        byte value = (byte)random.nextInt(5);
        return RaftMessageType.fromByte((byte) (value + 1));
    }

    private static LogEntry randomLogEntry(){
        byte[] value = new byte[random.nextInt(20) + 1];
        long term = random.nextLong();
        random.nextBytes(value);
        return new LogEntry(term, value, LogValueType.fromByte((byte)(random.nextInt(4) + 1)));
    }

    static class Pair<T1, T2>{
        private T1 item1;
        private T2 item2;

        public Pair(T1 item1, T2 item2){
            this.item1 = item1;
            this.item2 = item2;
        }
    }
}
