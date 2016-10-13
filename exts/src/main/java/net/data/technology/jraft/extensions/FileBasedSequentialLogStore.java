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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import net.data.technology.jraft.LogEntry;
import net.data.technology.jraft.LogValueType;
import net.data.technology.jraft.SequentialLogStore;

public class FileBasedSequentialLogStore implements SequentialLogStore {

    private static final String LOG_INDEX_FILE = "store.idx";
    private static final String LOG_STORE_FILE = "store.data";
    private static final String LOG_START_INDEX_FILE = "store.sti";
    private static final String LOG_INDEX_FILE_BAK = "store.idx.bak";
    private static final String LOG_STORE_FILE_BAK = "store.data.bak";
    private static final String LOG_START_INDEX_FILE_BAK = "store.sti.bak";
    private static final LogEntry zeroEntry = new LogEntry();
    private static final int BUFFER_SIZE = 1000;

    private Logger logger;
    private RandomAccessFile indexFile;
    private RandomAccessFile dataFile;
    private RandomAccessFile startIndexFile;
    private long entriesInStore;
    private long startIndex;
    private Path logContainer;
    private ReentrantReadWriteLock storeLock;
    private ReadLock storeReadLock;
    private WriteLock storeWriteLock;
    private LogBuffer buffer;
    private int bufferSize;

    public FileBasedSequentialLogStore(String logContainer){
        this(logContainer, BUFFER_SIZE);
    }
    
    public FileBasedSequentialLogStore(String logContainer, int bufferSize){
        this.storeLock = new ReentrantReadWriteLock();
        this.storeReadLock = this.storeLock.readLock();
        this.storeWriteLock = this.storeLock.writeLock();
        this.logContainer = Paths.get(logContainer);
        this.bufferSize = bufferSize;
        this.logger = LogManager.getLogger(getClass());
        try{
            this.indexFile = new RandomAccessFile(this.logContainer.resolve(LOG_INDEX_FILE).toString(), "rw");
            this.dataFile = new RandomAccessFile(this.logContainer.resolve(LOG_STORE_FILE).toString(), "rw");
            this.startIndexFile = new RandomAccessFile(this.logContainer.resolve(LOG_START_INDEX_FILE).toString(), "rw");
            if(this.startIndexFile.length() == 0){
                this.startIndex = 1;
                this.startIndexFile.writeLong(this.startIndex);
            }else{
                this.startIndex = this.startIndexFile.readLong();
            }

            this.entriesInStore = this.indexFile.length() / Long.BYTES;
            this.buffer = new LogBuffer(this.entriesInStore > this.bufferSize ? (this.entriesInStore + this.startIndex - this.bufferSize) : this.startIndex, this.bufferSize);
            this.fillBuffer();
            this.logger.debug(String.format("log store started with entriesInStore=%d, startIndex=%d", this.entriesInStore, this.startIndex));
        }catch(IOException exception){
            this.logger.error("failed to access log store", exception);
        }
    }

    @Override
    public long getFirstAvailableIndex() {
        try{
            this.storeReadLock.lock();
            return this.entriesInStore + this.startIndex;
        }finally{
            this.storeReadLock.unlock();
        }
    }

    @Override
    public long getStartIndex() {
        try{
            this.storeReadLock.lock();
            return this.startIndex;
        }finally{
            this.storeReadLock.unlock();
        }
    }

    @Override
    public LogEntry getLastLogEntry() {
        LogEntry lastEntry = this.buffer.lastEntry();
        return lastEntry == null ? zeroEntry : lastEntry;
    }

    @Override
    public long append(LogEntry logEntry) {
        try{
            this.storeWriteLock.lock();
            this.indexFile.seek(this.indexFile.length());
            long dataFileLength = this.dataFile.length();
            this.indexFile.writeLong(dataFileLength);
            this.dataFile.seek(dataFileLength);
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + 1 + logEntry.getValue().length);
            buffer.putLong(logEntry.getTerm());
            buffer.put(logEntry.getValueType().toByte());
            buffer.put(logEntry.getValue());
            this.dataFile.write(buffer.array());
            this.entriesInStore += 1;
            this.buffer.append(logEntry);
            return this.entriesInStore + this.startIndex - 1;
        }catch(IOException exception){
            this.logger.error("failed to append a log entry to store", exception);
            throw new RuntimeException(exception.getMessage(), exception);
        }finally{
            this.storeWriteLock.unlock();
        }
    }

    /**
     * write the log entry at the specific index, all log entries after index will be discarded
     * @param logIndex must be >= this.getStartIndex()
     * @param logEntry
     */
    @Override
    public void writeAt(long logIndex, LogEntry logEntry) {
        this.throwWhenNotInRange(logIndex);

        try{
            this.storeWriteLock.lock();
            long index = logIndex - this.startIndex + 1; //start index is one based
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + 1 + logEntry.getValue().length);
            buffer.putLong(logEntry.getTerm());
            buffer.put(logEntry.getValueType().toByte());
            buffer.put(logEntry.getValue());

            // find the positions for index and data files
            long dataPosition = this.dataFile.length();
            long indexPosition = (index - 1) * Long.BYTES;
            if(indexPosition < this.indexFile.length()){
                this.indexFile.seek(indexPosition);
                dataPosition = this.indexFile.readLong();
            }

            // write the data at the specified position
            this.indexFile.seek(indexPosition);
            this.dataFile.seek(dataPosition);
            this.indexFile.writeLong(dataPosition);
            this.dataFile.write(buffer.array());

            // trim the files if necessary
            if(this.indexFile.length() > this.indexFile.getFilePointer()){
                this.indexFile.setLength(this.indexFile.getFilePointer());
            }

            if(this.dataFile.length() > this.dataFile.getFilePointer()){
                this.dataFile.setLength(this.dataFile.getFilePointer());
            }

            if(index <= this.entriesInStore){
                this.buffer.trim(logIndex);
            }
            
            this.buffer.append(logEntry);
            this.entriesInStore = index;
        }catch(IOException exception){
            this.logger.error("failed to write a log entry at a specific index to store", exception);
            throw new RuntimeException(exception.getMessage(), exception);
        }finally{
            this.storeWriteLock.unlock();
        }
    }

    @Override
    public LogEntry[] getLogEntries(long startIndex, long endIndex) {
        this.throwWhenNotInRange(startIndex);

        // start and adjustedEnd are zero based, targetEndIndex is this.startIndex based
        long start, adjustedEnd, targetEndIndex;
        try{
            this.storeReadLock.lock();
            start = startIndex - this.startIndex;
            adjustedEnd = endIndex - this.startIndex;
            adjustedEnd = adjustedEnd > this.entriesInStore ? this.entriesInStore : adjustedEnd;
            targetEndIndex = endIndex > this.entriesInStore + this.startIndex + 1 ? this.entriesInStore + this.startIndex + 1 : endIndex;
        }finally{
            this.storeReadLock.unlock();
        }
        
        try{
            LogEntry[] entries = new LogEntry[(int)(adjustedEnd - start)];
            if(entries.length == 0){
                return entries;
            }

            // fill with buffer
            long bufferFirstIndex = this.buffer.fill(startIndex, targetEndIndex, entries);
            
            // Assumption: buffer.lastIndex() == this.entriesInStore + this.startIndex
            // (Yes, for sure, we need to enforce this assumption to be true)
            if(startIndex < bufferFirstIndex){
                // in this case, we need to read from store file
                try{
                    // we need to move the file pointer
                    this.storeWriteLock.lock();
                    long end = bufferFirstIndex - this.startIndex;
                    this.indexFile.seek(start * Long.BYTES);
                    long dataStart = this.indexFile.readLong();
                    for(int i = 0; i < (int)(end - start); ++i){
                        long dataEnd = this.indexFile.readLong();
                        int dataSize = (int)(dataEnd - dataStart);
                        byte[] logData = new byte[dataSize];
                        this.dataFile.seek(dataStart);
                        this.read(this.dataFile, logData);
                        entries[i] = new LogEntry(BinaryUtils.bytesToLong(logData, 0), Arrays.copyOfRange(logData, Long.BYTES + 1, logData.length), LogValueType.fromByte(logData[Long.BYTES]));
                        dataStart = dataEnd;
                    }
                }finally{
                    this.storeWriteLock.unlock();
                }
            }

            return entries;
        }catch(IOException exception){
            this.logger.error("failed to read entries from store", exception);
            throw new RuntimeException(exception.getMessage(), exception);
        }
    }

    @Override
    public LogEntry getLogEntryAt(long logIndex) {
        this.throwWhenNotInRange(logIndex);

        long index = 0;
        try{
            this.storeReadLock.lock();
            index = logIndex - this.startIndex + 1;
            if(index > this.entriesInStore){
                return null;
            }
        }finally{
            this.storeReadLock.unlock();
        }

        LogEntry entry = this.buffer.entryAt(logIndex);
        if(entry != null){
            return entry;
        }
        
        try{
            this.storeWriteLock.lock();
            long indexPosition = (index - 1) * Long.BYTES;
            this.indexFile.seek(indexPosition);
            long dataPosition = this.indexFile.readLong();
            long endDataPosition = this.indexFile.readLong();
            this.dataFile.seek(dataPosition);
            byte[] logData = new byte[(int)(endDataPosition - dataPosition)];
            this.read(this.dataFile, logData);
            return new LogEntry(BinaryUtils.bytesToLong(logData, 0), Arrays.copyOfRange(logData, Long.BYTES + 1, logData.length), LogValueType.fromByte(logData[Long.BYTES]));
        }catch(IOException exception){
            this.logger.error("failed to read files to get the specified entry");
            throw new RuntimeException(exception.getMessage(), exception);
        }finally{
            this.storeWriteLock.unlock();
        }
    }

    @Override
    public byte[] packLog(long logIndex, int itemsToPack){
        this.throwWhenNotInRange(logIndex);
        
        try{
            this.storeWriteLock.lock();
            long index = logIndex - this.startIndex + 1;
            if(index > this.entriesInStore){
                return new byte[0];
            }

            long endIndex = Math.min(index + itemsToPack, this.entriesInStore + 1);
            boolean readToEnd = (endIndex == this.entriesInStore + 1);
            long indexPosition = (index - 1) * Long.BYTES;
            this.indexFile.seek(indexPosition);
            byte[] indexBuffer = new byte[(int)(Long.BYTES * (endIndex - index))];
            this.read(this.indexFile, indexBuffer);
            long endOfLog = this.dataFile.length();
            if(!readToEnd){
                endOfLog = this.indexFile.readLong();
            }

            long startOfLog = BinaryUtils.bytesToLong(indexBuffer, 0);
            byte[] logBuffer = new byte[(int)(endOfLog - startOfLog)];
            this.dataFile.seek(startOfLog);
            this.read(this.dataFile, logBuffer);
            ByteArrayOutputStream memoryStream = new ByteArrayOutputStream();
            GZIPOutputStream gzipStream = new GZIPOutputStream(memoryStream);
            gzipStream.write(BinaryUtils.intToBytes(indexBuffer.length));
            gzipStream.write(BinaryUtils.intToBytes(logBuffer.length));
            gzipStream.write(indexBuffer);
            gzipStream.write(logBuffer);
            gzipStream.flush();
            memoryStream.flush();
            gzipStream.close();
            return memoryStream.toByteArray();
        }catch(IOException exception){
            this.logger.error("failed to read files to read data for packing");
            throw new RuntimeException(exception.getMessage(), exception);
        }finally{
            this.storeWriteLock.unlock();
        }
    }

    @Override
    public void applyLogPack(long logIndex, byte[] logPack){
        this.throwWhenNotInRange(logIndex);

        try{
            this.storeWriteLock.lock();
            long index = logIndex - this.startIndex + 1;
            ByteArrayInputStream memoryStream = new ByteArrayInputStream(logPack);
            GZIPInputStream gzipStream = new GZIPInputStream(memoryStream);
            byte[] sizeBuffer = new byte[Integer.BYTES];
            this.read(gzipStream, sizeBuffer);
            int indexDataSize = BinaryUtils.bytesToInt(sizeBuffer, 0);
            this.read(gzipStream, sizeBuffer);
            int logDataSize = BinaryUtils.bytesToInt(sizeBuffer, 0);
            byte[] indexBuffer = new byte[indexDataSize];
            this.read(gzipStream, indexBuffer);
            byte[] logBuffer = new byte[logDataSize];
            this.read(gzipStream, logBuffer);
            long indexFilePosition, dataFilePosition;
            if(index == this.entriesInStore + 1){
                indexFilePosition = this.indexFile.length();
                dataFilePosition = this.dataFile.length();
            }else{
                indexFilePosition = (index - 1) * Long.BYTES;
                this.indexFile.seek(indexFilePosition);
                dataFilePosition = this.indexFile.readLong();
            }

            this.indexFile.seek(indexFilePosition);
            this.indexFile.write(indexBuffer);
            this.indexFile.setLength(this.indexFile.getFilePointer());
            this.dataFile.seek(dataFilePosition);
            this.dataFile.write(logBuffer);
            this.dataFile.setLength(this.dataFile.getFilePointer());
            this.entriesInStore = index - 1 + indexBuffer.length / Long.BYTES;
            gzipStream.close();
            this.buffer.reset(this.entriesInStore > this.bufferSize ? this.entriesInStore + this.startIndex - this.bufferSize : this.startIndex);
            this.fillBuffer();
        }catch(IOException exception){
            this.logger.error("failed to write files to unpack logs for data");
            throw new RuntimeException(exception.getMessage(), exception);
        }finally{
            this.storeWriteLock.unlock();
        }
    }

    @Override
    public boolean compact(long lastLogIndex){
        this.throwWhenNotInRange(lastLogIndex);

        try{
            this.storeWriteLock.lock();
            this.backup();
            long lastIndex = lastLogIndex - this.startIndex;
            if(lastLogIndex >= this.getFirstAvailableIndex() - 1){
                this.indexFile.setLength(0);
                this.dataFile.setLength(0);
                this.startIndexFile.seek(0);
                this.startIndexFile.writeLong(lastLogIndex + 1);
                this.startIndex = lastLogIndex + 1;
                this.entriesInStore = 0;
                this.buffer.reset(lastLogIndex + 1);
                return true;
            }else{
                long dataPosition = -1;
                long indexPosition = Long.BYTES * (lastIndex + 1);
                byte[] dataPositionBuffer = new byte[Long.BYTES];
                this.indexFile.seek(indexPosition);
                this.read(this.indexFile, dataPositionBuffer);
                dataPosition = ByteBuffer.wrap(dataPositionBuffer).getLong();
                long indexFileNewLength = this.indexFile.length() - indexPosition;
                long dataFileNewLength = this.dataFile.length() - dataPosition;
    
                // copy the log data
                RandomAccessFile backupFile = new RandomAccessFile(this.logContainer.resolve(LOG_STORE_FILE_BAK).toString(), "r");
                FileChannel backupChannel = backupFile.getChannel();
                backupChannel.position(dataPosition);
                FileChannel channel = this.dataFile.getChannel();
                channel.transferFrom(backupChannel, 0, dataFileNewLength);
                this.dataFile.setLength(dataFileNewLength);
                backupFile.close();
    
                // copy the index data
                backupFile = new RandomAccessFile(this.logContainer.resolve(LOG_INDEX_FILE_BAK).toString(), "r");
                backupFile.seek(indexPosition);
                this.indexFile.seek(0);
                for(int i = 0; i < indexFileNewLength / Long.BYTES; ++i){
                    this.indexFile.writeLong(backupFile.readLong() - dataPosition);
                }
    
                this.indexFile.setLength(indexFileNewLength);
                backupFile.close();
    
                // save the starting index
                this.startIndexFile.seek(0);
                this.startIndexFile.write(ByteBuffer.allocate(Long.BYTES).putLong(lastLogIndex + 1).array());
                this.entriesInStore -= (lastLogIndex - this.startIndex + 1);
                this.startIndex = lastLogIndex + 1;
                this.buffer.reset(this.entriesInStore > this.bufferSize ? this.entriesInStore + this.startIndex - this.bufferSize : this.startIndex);
                this.fillBuffer();
                return true;
            }
        }catch(Throwable error){
            this.logger.error("fail to compact the logs due to error", error);
            this.restore();
            return false;
        }finally{
            this.storeWriteLock.unlock();
        }
    }

    public void close(){
        try{
            this.storeWriteLock.lock();
            this.dataFile.close();
            this.indexFile.close();
            this.startIndexFile.close();
        }catch(IOException exception){
            this.logger.error("failed to close data/index file(s)", exception);
        }finally{
            this.storeWriteLock.unlock();
        }
    }
    
    private void throwWhenNotInRange(long index){
        try{
            this.storeReadLock.lock();
            if(index < this.startIndex){
                throw new IllegalArgumentException("logIndex out of range");
            }
        }finally{
            this.storeReadLock.unlock();
        }
    }

    private void restore(){
        try{
            this.indexFile.close();
            this.dataFile.close();
            this.startIndexFile.close();
            Files.copy(this.logContainer.resolve(LOG_INDEX_FILE_BAK), this.logContainer.resolve(LOG_INDEX_FILE), StandardCopyOption.REPLACE_EXISTING);
            Files.copy(this.logContainer.resolve(LOG_STORE_FILE_BAK), this.logContainer.resolve(LOG_STORE_FILE), StandardCopyOption.REPLACE_EXISTING);
            Files.copy(this.logContainer.resolve(LOG_START_INDEX_FILE_BAK), this.logContainer.resolve(LOG_START_INDEX_FILE), StandardCopyOption.REPLACE_EXISTING);
            this.indexFile = new RandomAccessFile(this.logContainer.resolve(LOG_INDEX_FILE).toString(), "rw");
            this.dataFile = new RandomAccessFile(this.logContainer.resolve(LOG_STORE_FILE).toString(), "rw");
            this.startIndexFile = new RandomAccessFile(this.logContainer.resolve(LOG_START_INDEX_FILE).toString(), "rw");
        }catch(Exception error){
            // this is fatal...
            this.logger.fatal("cannot restore from failure, please manually restore the log files");
            System.exit(-1);
        }
    }

    private void backup(){
        try {
            Files.deleteIfExists(this.logContainer.resolve(LOG_INDEX_FILE_BAK));
            Files.deleteIfExists(this.logContainer.resolve(LOG_STORE_FILE_BAK));
            Files.deleteIfExists(this.logContainer.resolve(LOG_START_INDEX_FILE_BAK));
            Files.copy(this.logContainer.resolve(LOG_INDEX_FILE), this.logContainer.resolve(LOG_INDEX_FILE_BAK), StandardCopyOption.REPLACE_EXISTING);
            Files.copy(this.logContainer.resolve(LOG_STORE_FILE), this.logContainer.resolve(LOG_STORE_FILE_BAK), StandardCopyOption.REPLACE_EXISTING);
            Files.copy(this.logContainer.resolve(LOG_START_INDEX_FILE), this.logContainer.resolve(LOG_START_INDEX_FILE_BAK), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            this.logger.error("failed to create a backup folder", e);
            throw new RuntimeException("failed to create a backup folder");
        }
    }

    private void read(InputStream stream, byte[] buffer){
        try{
            int offset = 0;
            int bytesRead = 0;
            while(offset < buffer.length && (bytesRead = stream.read(buffer, offset, buffer.length - offset)) != -1){
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

    private void read(RandomAccessFile stream, byte[] buffer){
        try{
            int offset = 0;
            int bytesRead = 0;
            while(offset < buffer.length && (bytesRead = stream.read(buffer, offset, buffer.length - offset)) != -1){
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

    private void fillBuffer() throws IOException{
        long startIndex = this.buffer.firstIndex();
        long indexFileSize = this.indexFile.length();
        if(indexFileSize > 0){
            long indexPosition = (startIndex - this.startIndex) * Long.BYTES;
            this.indexFile.seek(indexPosition);
            byte[] indexData = new byte[(int)(indexFileSize - indexPosition)];
            this.read(this.indexFile, indexData);
            ByteBuffer indexBuffer = ByteBuffer.wrap(indexData);
            long dataStart = indexBuffer.getLong();
            this.dataFile.seek(dataStart);
            while(indexBuffer.hasRemaining()){
                long dataEnd = indexBuffer.getLong();
                this.buffer.append(this.readEntry((int)(dataEnd - dataStart)));
                dataStart = dataEnd;
            }
            
            // a little ugly, load last entry into buffer
            long dataEnd = this.dataFile.length();
            this.buffer.append(this.readEntry((int)(dataEnd - dataStart)));
        }
    }
    
    private LogEntry readEntry(int size){
        byte[] entryData = new byte[size];
        this.read(this.dataFile, entryData);
        ByteBuffer entryBuffer = ByteBuffer.wrap(entryData);
        long term = entryBuffer.getLong();
        byte valueType = entryBuffer.get();
        return new LogEntry(term, Arrays.copyOfRange(entryData, Long.BYTES + 1, entryData.length), LogValueType.fromByte(valueType));
    }
    
    public static class LogBuffer{

        private List<LogEntry> buffer;
        private ReentrantReadWriteLock bufferLock;
        private ReadLock bufferReadLock;
        private WriteLock bufferWriteLock;
        private long startIndex;
        private int maxSize;
        
        public LogBuffer(long startIndex, int maxSize){
            this.startIndex = startIndex;
            this.maxSize = maxSize;
            this.bufferLock = new ReentrantReadWriteLock();
            this.bufferReadLock = this.bufferLock.readLock();
            this.bufferWriteLock = this.bufferLock.writeLock();
            this.buffer = new ArrayList<LogEntry>();
        }
        
        public long lastIndex(){
            try{
                this.bufferReadLock.lock();
                return this.startIndex + this.buffer.size();
            }finally{
                this.bufferReadLock.unlock();
            }
        }
        
        public long firstIndex(){
            try{
                this.bufferReadLock.lock();
                return this.startIndex;
            }finally{
                this.bufferReadLock.unlock();
            }
        }
        
        public LogEntry lastEntry(){
            try{
                this.bufferReadLock.lock();
                if(this.buffer.size() > 0){
                    return this.buffer.get(this.buffer.size() - 1);
                }
                
                return null;
            }finally{
                this.bufferReadLock.unlock();
            }
        }
        
        public LogEntry entryAt(long index){
            try{
                this.bufferReadLock.lock();
                int indexWithinBuffer = (int)(index - this.startIndex);
                if(indexWithinBuffer >= this.buffer.size() || indexWithinBuffer < 0){
                    return null;
                }
                
                return this.buffer.get(indexWithinBuffer);
            }finally{
                this.bufferReadLock.unlock();
            }
        }
        
        // [start, end), returns the startIndex
        public long fill(long start, long end, LogEntry[] result){
            try{
                this.bufferReadLock.lock();
                if(end < this.startIndex){
                    return this.startIndex;
                }
                
                int offset = (int)(start - this.startIndex);
                if(offset > 0){
                    for(int i = 0; i < (int)(end - start); ++i, ++offset){
                        result[i] = this.buffer.get(offset);
                    }
                }else{
                    offset *= -1;
                    for(int i = 0; i < (int)(end - this.startIndex); ++i, ++offset){
                        result[offset] = this.buffer.get(i);
                    }
                }
                
                return this.startIndex;
            }finally{
                this.bufferReadLock.unlock();
            }
        }
        
        // trimming the buffer [fromIndex, end)
        public void trim(long fromIndex){
            try{
                this.bufferWriteLock.lock();
                int index = (int)(fromIndex - this.startIndex);
                if(index < this.buffer.size()){
                    this.buffer.subList(index, this.buffer.size()).clear();
                }
            }finally{
                this.bufferWriteLock.unlock();
            }
        }
        
        // trimming the buffer [fromIndex, endIndex)
        public void trim(long fromIndex, long endIndex){
            try{
                this.bufferWriteLock.lock();
                int index = (int)(fromIndex - this.startIndex);
                int end = (int)(endIndex - this.startIndex);
                if(index < this.buffer.size()){
                    this.buffer.subList(index, end).clear();
                }
                
                // we may need to reset the start index
                if(fromIndex == this.startIndex){
                    this.startIndex = endIndex > this.startIndex + end ? this.startIndex + end : endIndex;
                }
            }finally{
                this.bufferWriteLock.unlock();
            }
        }
        
        public void append(LogEntry entry){
            try{
                this.bufferWriteLock.lock();
                this.buffer.add(entry);
                if(this.maxSize < this.buffer.size()){
                    this.buffer.remove(0);
                    this.startIndex += 1;
                }
            }finally{
                this.bufferWriteLock.unlock();
            }
        }
        
        public void reset(long startIndex){
            try{
                this.bufferWriteLock.lock();
                this.buffer.clear();
                this.startIndex = startIndex;
            }finally{
                this.bufferWriteLock.unlock();
            }
        }
    }
}
