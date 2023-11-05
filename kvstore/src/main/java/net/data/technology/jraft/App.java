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
import java.io.OutputStream;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.StringTokenizer;
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
                .withSnapshotEnabled(0) //disable snapshots
                .withSyncSnapshotBlockSize(0);
        KVStore mp = new KVStore(baseDir, port);
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

    private static void executeAsClient(ClusterConfiguration configuration, ExecutorService executor) throws Exception {
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

                if(values.size() == 3) {
                    ClusterServer server = new ClusterServer();
                    server.setEndpoint(values.get(2));
                    server.setId(Integer.parseInt(values.get(1)));
                    boolean accepted = client.addServer(server).get();
                    System.out.println("Accepted: " + String.valueOf(accepted));
                    continue;
                }
            } else if(message.startsWith("rmsrv:")){
                String text = message.substring(6);
                int serverId = Integer.parseInt(text.trim());
                boolean accepted = client.removeServer(serverId).get();
                System.out.println("Accepted: " + String.valueOf(accepted));
                continue;
            }

            if (message.startsWith("put")) {
                if (message.split(":").length == 3) {
                    String entry = message.substring(message.indexOf(':') + 1);
                    boolean accepted = client.appendEntries(new byte[][]{ entry.getBytes() }).get();
                    System.out.println("Accepted: " + accepted);
                }
                continue;
            }

            if (message.startsWith("get")) {
                message = message.substring(message.indexOf(':') + 1);
                // send to state machine
                Socket socket = new Socket("127.0.0.1", 8001);
                OutputStream socketOutputStream = socket.getOutputStream();

                int msgLen = message.getBytes(StandardCharsets.UTF_8).length;
                byte[] bytes = new byte[msgLen + 4];
                for(int i = 0; i < 4; ++i){
                    int value = (msgLen >> (i * 8));
                    bytes[i] = (byte) (value & 0xFF);
                }

                System.arraycopy(message.getBytes(StandardCharsets.UTF_8), 0, bytes, 4, msgLen);

                socketOutputStream.write(bytes);

                System.out.println(new BufferedReader(new InputStreamReader(socket.getInputStream())).readLine());
                socket.close();
            }
        }
    }
}
