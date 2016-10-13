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

public enum LogValueType {

    Application {
        @Override
        public byte toByte(){
            return 1;
        }
    },
    Configuration {
        @Override
        public byte toByte(){
            return 2;
        }
    },
    ClusterServer {
        @Override
        public byte toByte(){
            return 3;
        }
    },
    LogPack {
        @Override
        public byte toByte(){
            return 4;
        }
    },
    SnapshotSyncRequest {
        @Override
        public byte toByte(){
            return 5;
        }
    };

    public abstract byte toByte();

    public static LogValueType fromByte(byte b){
        switch(b){
        case 1:
            return Application;
        case 2:
            return Configuration;
        case 3:
            return ClusterServer;
        case 4:
            return LogPack;
        case 5:
            return SnapshotSyncRequest;
        default:
            throw new IllegalArgumentException(String.format("%d is not defined for LogValueType", b));
        }
    }
}
