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

package net.data.technology.jraft.extensions;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import net.data.technology.jraft.ClusterConfiguration;
import net.data.technology.jraft.SequentialLogStore;
import net.data.technology.jraft.ServerState;
import net.data.technology.jraft.ServerStateManager;

public class FileBasedServerStateManager implements ServerStateManager {

    private static final String STATE_FILE = "server.state";
    private static final String CONFIG_FILE = "config.properties";
    private static final String CLUSTER_CONFIG_FILE = "cluster.json";

    private RandomAccessFile serverStateFile;
    private FileBasedSequentialLogStore logStore;
    private Logger logger;
    private Path container;
    private int serverId;

    public FileBasedServerStateManager(String dataDirectory){
        this.logStore = new FileBasedSequentialLogStore(dataDirectory);
        this.container = Paths.get(dataDirectory);
        this.logger = LogManager.getLogger(getClass());
        try{
            Properties props = new Properties();
            FileInputStream configInput = new FileInputStream(this.container.resolve(CONFIG_FILE).toString());
            props.load(configInput);
            String serverIdValue = props.getProperty("server.id");
            this.serverId = serverIdValue == null || serverIdValue.length() == 0 ? -1 : Integer.parseInt(serverIdValue.trim());
            configInput.close();
            this.serverStateFile = new RandomAccessFile(this.container.resolve(STATE_FILE).toString(), "rw");
            this.serverStateFile.seek(0);
        }catch(IOException exception){
            this.logger.error("failed to create/open server state file", exception);
            throw new IllegalArgumentException("cannot create/open the state file", exception);
        }
    }

    @Override
    public ClusterConfiguration loadClusterConfiguration(){
        Gson gson = new GsonBuilder().create();
        FileInputStream stream = null;

        try{
            stream = new FileInputStream(this.container.resolve(CLUSTER_CONFIG_FILE).toString());
            ClusterConfiguration config = gson.fromJson(new InputStreamReader(stream, StandardCharsets.UTF_8), ClusterConfiguration.class);
            return config;
        }catch(IOException error){
            this.logger.error("failed to read cluster configuration", error);
            throw new RuntimeException("failed to read in cluster config", error);
        }finally{
            if(stream != null){
                try{
                    stream.close();
                }catch(Exception e){
                    //ignore the error
                }
            }
        }
    }

    public void saveClusterConfiguration(ClusterConfiguration configuration){
        Gson gson = new GsonBuilder().create();
        String configData = gson.toJson(configuration);
        try {
            Files.deleteIfExists(this.container.resolve(CLUSTER_CONFIG_FILE));
            FileOutputStream output = new FileOutputStream(this.container.resolve(CLUSTER_CONFIG_FILE).toString());
            output.write(configData.getBytes(StandardCharsets.UTF_8));
            output.flush();
            output.close();
        } catch (IOException error) {
            this.logger.error("failed to save cluster config to file", error);
        }
    }

    public int getServerId(){
        return this.serverId;
    }

    @Override
    public synchronized void persistState(ServerState serverState) {
        try{
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2 + Integer.BYTES);
            buffer.putLong(serverState.getTerm());
            buffer.putLong(serverState.getCommitIndex());
            buffer.putInt(serverState.getVotedFor());
            this.serverStateFile.write(buffer.array());
            this.serverStateFile.seek(0);
        }catch(IOException ioError){
            this.logger.error("failed to write to the server state file", ioError);
            throw new RuntimeException("fatal I/O error while writing to the state file", ioError);
        }
    }

    @Override
    public synchronized ServerState readState() {
        try{
            if(this.serverStateFile.length() == 0){
                return null;
            }

            byte[] stateData = new byte[Long.BYTES * 2 + Integer.BYTES];
            this.read(stateData);
            this.serverStateFile.seek(0);
            ByteBuffer buffer = ByteBuffer.wrap(stateData);
            ServerState state = new ServerState();
            state.setTerm(buffer.getLong());
            state.setCommitIndex(buffer.getLong());
            state.setVotedFor(buffer.getInt());
            return state;
        }catch(IOException ioError){
            this.logger.error("failed to read from the server state file", ioError);
            throw new RuntimeException("fatal I/O error while reading from state file", ioError);
        }
    }

    @Override
    public SequentialLogStore loadLogStore() {
        return this.logStore;
    }

    public void close(){
        try{
            this.serverStateFile.close();
            this.logStore.close();
        }catch(IOException exception){
            this.logger.info("failed to shutdown the server state manager due to io error", exception);
        }
    }

    private void read(byte[] buffer){
        try{
            int offset = 0;
            int bytesRead = 0;
            while(offset < buffer.length && (bytesRead = this.serverStateFile.read(buffer, offset, buffer.length - offset)) != -1){
                offset += bytesRead;
            }

            if(offset < buffer.length){
                this.logger.error(String.format("only %d bytes are read while %d bytes are desired, bad file", offset, buffer.length));
                throw new RuntimeException("bad file, insufficient file data for reading");
            }
        }catch(IOException exception){
            this.logger.error("failed to read and fill the buffer", exception);
            throw new RuntimeException(exception.getMessage(), exception);
        }
    }
}
