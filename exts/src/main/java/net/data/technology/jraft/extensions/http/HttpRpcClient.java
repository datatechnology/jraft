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

package net.data.technology.jraft.extensions.http;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import net.data.technology.jraft.RaftRequestMessage;
import net.data.technology.jraft.RaftResponseMessage;
import net.data.technology.jraft.RpcClient;

public class HttpRpcClient implements RpcClient {
    private String serverUrl;
    private CloseableHttpAsyncClient httpClient;
    private Gson gson;
    private Logger logger;

    public HttpRpcClient(String serverUrl){
        this.serverUrl = serverUrl;
        this.httpClient = HttpAsyncClients.createDefault();
        this.gson = new GsonBuilder().create();
        this.logger = LogManager.getLogger(getClass());
    }

    @Override
    public CompletableFuture<RaftResponseMessage> send(RaftRequestMessage request) {
        CompletableFuture<RaftResponseMessage> future = new CompletableFuture<RaftResponseMessage>();
        String payload = this.gson.toJson(request);
        HttpPost postRequest = new HttpPost(this.serverUrl);
        postRequest.setEntity(new StringEntity(payload, StandardCharsets.UTF_8));
        this.httpClient.execute(postRequest, new FutureCallback<HttpResponse>(){

            @Override
            public void completed(HttpResponse result) {
                if(result.getStatusLine().getStatusCode() != 200){
                    logger.info("receive an response error code " + String.valueOf(result.getStatusLine().getStatusCode()) + " from server");
                    future.completeExceptionally(new IOException("Service Error"));
                }

                try{
                    InputStreamReader reader = new InputStreamReader(result.getEntity().getContent());
                    RaftResponseMessage response = gson.fromJson(reader, RaftResponseMessage.class);
                    future.complete(response);
                }catch(Throwable error){
                    logger.info("fails to parse the response from server due to errors", error);
                    future.completeExceptionally(error);
                }
            }

            @Override
            public void failed(Exception ex) {
                future.completeExceptionally(ex);
            }

            @Override
            public void cancelled() {
                future.completeExceptionally(new IOException("request cancelled"));
            }});
        return future;
    }

}
