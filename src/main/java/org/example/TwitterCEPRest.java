package org.example;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

/**
 * 
 * REST facade of TwitterCEP
 *
 */
@Path("/")
public class TwitterCEPRest {
    
    private TwitterCEPDelegate delegate = TwitterCEPDelegate.getInstance(); // singleton

    @GET
    @Path("/start")
    @Produces({ "text/html" })
    public String start() {
        delegate.startKnowledgeAgent();
        delegate.run();
        return "kagent and ksession is running";
    }
    
    @GET
    @Path("/refresh")
    @Produces({ "text/html" })
    public String refresh() {
        delegate.stopKnowledgeSession();
        delegate.run();
        return "A new ksession is created and running";
    }
    
    @GET
    @Path("/stop")
    @Produces({ "text/html" })
    public String stop() {
        StringBuilder sb = new StringBuilder();
        String result = delegate.poll();
        sb.append("kagent and ksession stopped<br>");
        sb.append("-------------------------<br>");
        sb.append(result);
        delegate.stopKnowledgeSession();
        delegate.stopKnowledgeAgent();
        return sb.toString();
    }
    
    @GET
    @Path("/poll")
    @Produces({ "text/html" })
    public String poll() {
        return delegate.poll();
    }

}
