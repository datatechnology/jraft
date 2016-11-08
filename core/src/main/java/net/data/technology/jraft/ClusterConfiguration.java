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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Cluster configuration, a class to hold the cluster configuration information
 * @author Data Technology LLC
 *
 */
public class ClusterConfiguration {

    private long logIndex;
    private long lastLogIndex;
    private List<ClusterServer> servers;

    public ClusterConfiguration(){
        this.servers = new LinkedList<ClusterServer>();
        this.logIndex = 0;
        this.lastLogIndex = 0;
    }

    /**
     * De-serialize the data stored in buffer to cluster configuration
     * this is used for the peers to get the cluster configuration from log entry value
     * @param buffer the binary data
     * @return cluster configuration
     */
    public static ClusterConfiguration fromBytes(ByteBuffer buffer){
        ClusterConfiguration configuration = new ClusterConfiguration();
        configuration.setLogIndex(buffer.getLong());
        configuration.setLastLogIndex(buffer.getLong());
        while(buffer.hasRemaining()){
            configuration.getServers().add(new ClusterServer(buffer));
        }

        return configuration;
    }

    /**
     * De-serialize the data stored in buffer to cluster configuration
     * this is used for the peers to get the cluster configuration from log entry value
     * @param buffer the binary data
     * @return cluster configuration
     */
    public static ClusterConfiguration fromBytes(byte[] data){
        return fromBytes(ByteBuffer.wrap(data));
    }

    public long getLogIndex() {
        return logIndex;
    }

    public void setLogIndex(long logIndex) {
        this.logIndex = logIndex;
    }

    /**
     * Gets the log index that contains the previous cluster configuration
     * @return log index
     */
    public long getLastLogIndex() {
        return lastLogIndex;
    }

    public void setLastLogIndex(long lastLogIndex) {
        this.lastLogIndex = lastLogIndex;
    }

    public List<ClusterServer> getServers() {
        return servers;
    }

    /**
     * Try to get a cluster server configuration from cluster configuration
     * @param id the server id
     * @return a cluster server configuration or null if id is not found
     */
    public ClusterServer getServer(int id){
        for(ClusterServer server : this.servers){
            if(server.getId() == id){
                return server;
            }
        }

        return null;
    }

    /**
     * Serialize the cluster configuration into a buffer
     * this is used for the leader to serialize a new cluster configuration and replicate to peers
     * @return binary data that represents the cluster configuration
     */
    public byte[] toBytes(){
        int totalSize = Long.BYTES * 2;
        List<byte[]> serversData = new ArrayList<byte[]>(this.servers.size());
        for(int i = 0; i < this.servers.size(); ++i){
            ClusterServer server = this.servers.get(i);
            byte[] dataForServer = server.toBytes();
            totalSize += dataForServer.length;
            serversData.add(dataForServer);
        }

        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.putLong(this.logIndex);
        buffer.putLong(this.lastLogIndex);
        for(int i = 0; i < serversData.size(); ++i){
            buffer.put(serversData.get(i));
        }

        return buffer.array();
    }
}
