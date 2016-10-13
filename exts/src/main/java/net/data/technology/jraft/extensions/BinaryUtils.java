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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.LogManager;

import net.data.technology.jraft.LogEntry;
import net.data.technology.jraft.LogValueType;
import net.data.technology.jraft.RaftMessageType;
import net.data.technology.jraft.RaftRequestMessage;
import net.data.technology.jraft.RaftResponseMessage;

public class BinaryUtils {

    public static final int RAFT_RESPONSE_HEADER_SIZE = Integer.BYTES * 2 + Long.BYTES * 2 + 2;
    public static final int RAFT_REQUEST_HEADER_SIZE = Integer.BYTES * 3 + Long.BYTES * 4 + 1;

    public static byte[] longToBytes(long value){
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(value);
        return buffer.array();
    }

    public static long bytesToLong(byte[] bytes, int offset){
        ByteBuffer buffer = ByteBuffer.wrap(bytes, offset, Long.BYTES);
        return buffer.getLong();
    }

    public static byte[] intToBytes(int value){
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(value);
        return buffer.array();
    }

    public static int bytesToInt(byte[] bytes, int offset){
        ByteBuffer buffer = ByteBuffer.wrap(bytes, offset, Integer.BYTES);
        return buffer.getInt();
    }

    public static byte booleanToByte(boolean value){
        return value ? (byte)1 : (byte)0;
    }

    public static boolean byteToBoolean(byte value){
        return value != 0;
    }

    public static byte[] messageToBytes(RaftResponseMessage response){
        ByteBuffer buffer = ByteBuffer.allocate(RAFT_RESPONSE_HEADER_SIZE);
        buffer.put(response.getMessageType().toByte());
        buffer.put(intToBytes(response.getSource()));
        buffer.put(intToBytes(response.getDestination()));
        buffer.put(longToBytes(response.getTerm()));
        buffer.put(longToBytes(response.getNextIndex()));
        buffer.put(booleanToByte(response.isAccepted()));
        return buffer.array();
    }

    public static RaftResponseMessage bytesToResponseMessage(byte[] data){
        if(data == null || data.length != RAFT_RESPONSE_HEADER_SIZE){
            throw new IllegalArgumentException(String.format("data must have %d bytes for a raft response message", RAFT_RESPONSE_HEADER_SIZE));
        }

        ByteBuffer buffer = ByteBuffer.wrap(data);
        RaftResponseMessage response = new RaftResponseMessage();
        response.setMessageType(RaftMessageType.fromByte(buffer.get()));
        response.setSource(buffer.getInt());
        response.setDestination(buffer.getInt());
        response.setTerm(buffer.getLong());
        response.setNextIndex(buffer.getLong());
        response.setAccepted(buffer.get() == 1);
        return response;
    }

    public static byte[] messageToBytes(RaftRequestMessage request){
        LogEntry[] logEntries = request.getLogEntries();
        int logSize = 0;
        List<byte[]> buffersForLogs = null;
        if(logEntries != null && logEntries.length > 0){
            buffersForLogs = new ArrayList<byte[]>(logEntries.length);
            for(LogEntry logEntry : logEntries){
                byte[] logData = logEntryToBytes(logEntry);
                logSize += logData.length;
                buffersForLogs.add(logData);
            }
        }

        ByteBuffer requestBuffer = ByteBuffer.allocate(RAFT_REQUEST_HEADER_SIZE + logSize);
        requestBuffer.put(request.getMessageType().toByte());
        requestBuffer.put(intToBytes(request.getSource()));
        requestBuffer.put(intToBytes(request.getDestination()));
        requestBuffer.put(longToBytes(request.getTerm()));
        requestBuffer.put(longToBytes(request.getLastLogTerm()));
        requestBuffer.put(longToBytes(request.getLastLogIndex()));
        requestBuffer.put(longToBytes(request.getCommitIndex()));
        requestBuffer.put(intToBytes(logSize));
        if(buffersForLogs != null){
            for(byte[] logData : buffersForLogs){
                requestBuffer.put(logData);
            }
        }

        return requestBuffer.array();
    }

    public static Pair<RaftRequestMessage, Integer> bytesToRequestMessage(byte[] data){
        if(data == null || data.length != RAFT_REQUEST_HEADER_SIZE){
            throw new IllegalArgumentException("invalid request message header.");
        }

        ByteBuffer buffer = ByteBuffer.wrap(data);
        RaftRequestMessage request = new RaftRequestMessage();
        request.setMessageType(RaftMessageType.fromByte(buffer.get()));
        request.setSource(buffer.getInt());
        request.setDestination(buffer.getInt());
        request.setTerm(buffer.getLong());
        request.setLastLogTerm(buffer.getLong());
        request.setLastLogIndex(buffer.getLong());
        request.setCommitIndex(buffer.getLong());
        int logDataSize = buffer.getInt();
        return new Pair<RaftRequestMessage, Integer>(request, logDataSize);
    }

    public static byte[] logEntryToBytes(LogEntry logEntry){
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try{
            output.write(longToBytes(logEntry.getTerm()));
            output.write(logEntry.getValueType().toByte());
            output.write(intToBytes(logEntry.getValue().length));
            output.write(logEntry.getValue());
            output.flush();
            return output.toByteArray();
        }catch(IOException exception){
            LogManager.getLogger("BinaryUtil").error("failed to serialize LogEntry to memory", exception);
            throw new RuntimeException("Running into bad situation, where memory may not be sufficient", exception);
        }
    }

    public static LogEntry[] bytesToLogEntries(byte[] data){
        if(data == null || data.length < Long.BYTES + Integer.BYTES){
            throw new IllegalArgumentException("invalid log entries data");
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        List<LogEntry> logEntries = new ArrayList<LogEntry>();
        while(buffer.hasRemaining()){
            long term = buffer.getLong();
            byte valueType = buffer.get();
            int valueSize = buffer.getInt();
            byte[] value = new byte[valueSize];
            if(valueSize > 0){
                buffer.get(value);
            }
            logEntries.add(new LogEntry(term, value, LogValueType.fromByte(valueType)));
        }

        return logEntries.toArray(new LogEntry[0]);
    }
}
