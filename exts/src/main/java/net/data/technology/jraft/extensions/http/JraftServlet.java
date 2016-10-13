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

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import net.data.technology.jraft.RaftMessageHandler;
import net.data.technology.jraft.RaftRequestMessage;
import net.data.technology.jraft.RaftResponseMessage;

public class JraftServlet extends HttpServlet {

    private static final long serialVersionUID = -414289039785671159L;
    private Logger logger;
    private Gson gson;

    public JraftServlet(){
        this.logger = LogManager.getLogger(getClass());
        this.gson = new GsonBuilder().create();
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException{
        response.getWriter().write("listening.");
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException{
        RaftMessageHandler messageHandler = JraftServletListener.getMessageHandler(this.getServletContext());
        if(messageHandler == null){
            this.logger.error("JraftServletListener is not setup correctly, no message handler could be found.");;
            response.setStatus(503); // Service is not available
            response.getWriter().print("Jraft is not yet ready to serve");
        }else{
            InputStreamReader reader = new InputStreamReader(request.getInputStream());
            RaftRequestMessage raftRequest = this.gson.fromJson(reader, RaftRequestMessage.class);
            RaftResponseMessage raftResponse = messageHandler.processRequest(raftRequest);
            response.setStatus(200);
            response.setContentType("application/json");
            response.getWriter().write(this.gson.toJson(raftResponse));
        }
    }

}
