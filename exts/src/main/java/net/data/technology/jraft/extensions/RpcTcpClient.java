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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import net.data.technology.jraft.RaftRequestMessage;
import net.data.technology.jraft.RaftResponseMessage;
import net.data.technology.jraft.RpcClient;

public class RpcTcpClient implements RpcClient {

    private AsynchronousSocketChannel connection;
    private AsynchronousChannelGroup channelGroup;
    private ConcurrentLinkedQueue<AsyncTask<ByteBuffer>> readTasks;
    private ConcurrentLinkedQueue<AsyncTask<RaftRequestMessage>> writeTasks;
    private AtomicInteger readers;
    private AtomicInteger writers;
    private InetSocketAddress remote;
    private Logger logger;

    public RpcTcpClient(InetSocketAddress remote, ExecutorService executorService){
        this.remote = remote;
        this.logger = LogManager.getLogger(getClass());
        this.readTasks = new ConcurrentLinkedQueue<AsyncTask<ByteBuffer>>();
        this.writeTasks = new ConcurrentLinkedQueue<AsyncTask<RaftRequestMessage>>();
        this.readers = new AtomicInteger(0);
        this.writers = new AtomicInteger(0);
        try{
            this.channelGroup = AsynchronousChannelGroup.withThreadPool(executorService);
        }catch(IOException err){
            this.logger.error("failed to create channel group", err);
            throw new RuntimeException("failed to create the channel group due to errors.");
        }
    }

    @Override
    public synchronized CompletableFuture<RaftResponseMessage> send(final RaftRequestMessage request) {
        this.logger.debug(String.format("trying to send message %s to server %d at endpoint %s", request.getMessageType().toString(), request.getDestination(), this.remote.toString()));
        CompletableFuture<RaftResponseMessage> result = new CompletableFuture<RaftResponseMessage>();
        if(this.connection == null || !this.connection.isOpen()){
            try{
                this.connection = AsynchronousSocketChannel.open(this.channelGroup);
                this.connection.connect(this.remote, new AsyncTask<RaftRequestMessage>(request, result), handlerFrom((Void v, AsyncTask<RaftRequestMessage> task) -> {
                    sendAndRead(task, false);
                }));
            }catch(Throwable error){
                closeSocket();
                result.completeExceptionally(error);
            }
        }else{
            this.sendAndRead(new AsyncTask<RaftRequestMessage>(request, result), false);
        }

        return result;
    }

    private void sendAndRead(AsyncTask<RaftRequestMessage> task, boolean skipQueueing){
        if(!skipQueueing){
            int writerCount = this.writers.getAndIncrement();
            if(writerCount > 0){
                this.logger.debug("there is a pending write, queue this write task");
                this.writeTasks.add(task);
                return;
            }
        }

        ByteBuffer buffer = ByteBuffer.wrap(BinaryUtils.messageToBytes(task.input));
        try{
            AsyncUtility.writeToChannel(this.connection, buffer, task, handlerFrom((Integer bytesSent, AsyncTask<RaftRequestMessage> context) -> {
                if(bytesSent.intValue() < buffer.limit()){
                    logger.info("failed to sent the request to remote server.");
                    context.future.completeExceptionally(new IOException("Only partial of the data could be sent"));
                    closeSocket();
                }else{
                    // read the response
                    ByteBuffer responseBuffer = ByteBuffer.allocate(BinaryUtils.RAFT_RESPONSE_HEADER_SIZE);
                    this.readResponse(new AsyncTask<ByteBuffer>(responseBuffer, context.future), false);
                }

                int waitingWriters = this.writers.decrementAndGet();
                if(waitingWriters > 0){
                    this.logger.debug("there are pending writers in queue, will try to process them");
                    AsyncTask<RaftRequestMessage> pendingTask = null;
                    while((pendingTask = this.writeTasks.poll()) == null);
                    this.sendAndRead(pendingTask, true);
                }
            }));
        }catch(Exception writeError){
            logger.info("failed to write the socket", writeError);
            task.future.completeExceptionally(writeError);
            closeSocket();
        }
    }

    private void readResponse(AsyncTask<ByteBuffer> task, boolean skipQueueing){
        if(!skipQueueing){
            int readerCount = this.readers.getAndIncrement();
            if(readerCount > 0){
                this.logger.debug("there is a pending read, queue this read task");
                this.readTasks.add(task);
                return;
            }
        }

        CompletionHandler<Integer, AsyncTask<ByteBuffer>> handler = handlerFrom((Integer bytesRead, AsyncTask<ByteBuffer> context) -> {
            if(bytesRead.intValue() < BinaryUtils.RAFT_RESPONSE_HEADER_SIZE){
                logger.info("failed to read response from remote server.");
                context.future.completeExceptionally(new IOException("Only part of the response data could be read"));
                closeSocket();
            }else{
                RaftResponseMessage response = BinaryUtils.bytesToResponseMessage(context.input.array());
                context.future.complete(response);
            }

            int waitingReaders = this.readers.decrementAndGet();
            if(waitingReaders > 0){
                this.logger.debug("there are pending readers in queue, will try to process them");
                AsyncTask<ByteBuffer> pendingTask = null;
                while((pendingTask = this.readTasks.poll()) == null);
                this.readResponse(pendingTask, true);
            }
        });

        try{
            this.logger.debug("reading response from socket...");
            AsyncUtility.readFromChannel(this.connection, task.input, task, handler);
        }catch(Exception readError){
            logger.info("failed to read from socket", readError);
            task.future.completeExceptionally(readError);
            closeSocket();
        }
    }

    private <V, I> CompletionHandler<V, AsyncTask<I>> handlerFrom(BiConsumer<V, AsyncTask<I>> completed) {
        return AsyncUtility.handlerFrom(completed, (Throwable error, AsyncTask<I> context) -> {
                        this.logger.info("socket error", error);
                        context.future.completeExceptionally(error);
                        closeSocket();
                    });
    }

    private synchronized void closeSocket(){
        this.logger.debug("close the socket due to errors");
        try{
            if(this.connection != null){
                this.connection.close();
                this.connection = null;
            }
        }catch(IOException ex){
            this.logger.info("failed to close socket", ex);
        }

        while(true){
            AsyncTask<ByteBuffer> task = this.readTasks.poll();
            if(task == null){
                break;
            }

            task.future.completeExceptionally(new IOException("socket is closed, no more reads can be completed"));
        }

        while(true){
            AsyncTask<RaftRequestMessage> task = this.writeTasks.poll();
            if(task == null){
                break;
            }

            task.future.completeExceptionally(new IOException("socket is closed, no more writes can be completed"));
        }

        this.readers.set(0);
        this.writers.set(0);
    }

    static class AsyncTask<TInput>{
        private TInput input;
        private CompletableFuture<RaftResponseMessage> future;

        public AsyncTask(TInput input, CompletableFuture<RaftResponseMessage> future){
            this.input = input;
            this.future = future;
        }
    }
}
