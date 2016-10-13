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

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousByteChannel;
import java.nio.channels.CompletionHandler;
import java.util.function.BiConsumer;

public class AsyncUtility {

    public static <V, A> CompletionHandler<V, A> handlerFrom(BiConsumer<V, A> completed, BiConsumer<Throwable, A> failed) {
        return new CompletionHandler<V, A>() {
            @Override
            public void completed(V result, A attachment) {
                completed.accept(result, attachment);
            }

            @Override
            public void failed(Throwable exc, A attachment) {
                failed.accept(exc, attachment);
            }
        };
    }

    public static <A> void readFromChannel(AsynchronousByteChannel channel, ByteBuffer buffer, A attachment, CompletionHandler<Integer, A> completionHandler){
        try{
            channel.read(
                    buffer,
                    new AsyncContext<A>(attachment, completionHandler),
                    handlerFrom(
                    (Integer result, AsyncContext<A> a) -> {
                        int bytesRead = result.intValue();
                        if(bytesRead == -1 || !buffer.hasRemaining()){
                            a.completionHandler.completed(buffer.position(), a.attachment);
                        }else{
                            readFromChannel(channel, buffer, a.attachment, a.completionHandler);
                        }
                    },
                    (Throwable error, AsyncContext<A> a) -> {
                        a.completionHandler.failed(error, a.attachment);
                    }));
        }catch(Throwable exception){
            completionHandler.failed(exception, attachment);
        }
    }

    public static <A> void writeToChannel(AsynchronousByteChannel channel, ByteBuffer buffer, A attachment, CompletionHandler<Integer, A> completionHandler){
        try{
            channel.write(
                    buffer,
                    new AsyncContext<A>(attachment, completionHandler),
                    handlerFrom(
                    (Integer result, AsyncContext<A> a) -> {
                        int bytesRead = result.intValue();
                        if(bytesRead == -1 || !buffer.hasRemaining()){
                            a.completionHandler.completed(buffer.position(), a.attachment);
                        }else{
                            writeToChannel(channel, buffer, a.attachment, a.completionHandler);
                        }
                    },
                    (Throwable error, AsyncContext<A> a) -> {
                        a.completionHandler.failed(error, a.attachment);
                    }));
        }catch(Throwable exception){
            completionHandler.failed(exception, attachment);
        }
    }

    static class AsyncContext<A>{
        private A attachment;
        private CompletionHandler<Integer, A> completionHandler;

        public AsyncContext(A attachment, CompletionHandler<Integer, A> handler){
            this.attachment = attachment;
            this.completionHandler = handler;
        }
    }
}
