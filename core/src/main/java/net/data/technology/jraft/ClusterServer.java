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
import java.nio.charset.StandardCharsets;

/**
 * Cluster server configuration 
 * a class to hold the configuration information for a server in a cluster
 * @author Data Technology LLC
 *
 */
public class ClusterServer {

    private int id;
    private String endpoint;

    public ClusterServer(){
        this.id = -1;
        this.endpoint = null;
    }

    public ClusterServer(ByteBuffer buffer){
        this.id = buffer.getInt();
        int dataSize = buffer.getInt();
        byte[] endpointData = new byte[dataSize];
        buffer.get(endpointData);
        this.endpoint = new String(endpointData, StandardCharsets.UTF_8);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    /**
     * Serialize a server configuration to binary data
     * @return the binary data that represents the server configuration
     */
    public byte[] toBytes(){
        byte[] endpointData = this.endpoint.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES * 2 + endpointData.length);
        buffer.putInt(this.id);
        buffer.putInt(endpointData.length);
        buffer.put(endpointData);
        return buffer.array();
    }
}
