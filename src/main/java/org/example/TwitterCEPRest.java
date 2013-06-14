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
        try {
            delegate.startKnowledgeAgent();
            delegate.run();
            return "<div class='alert alert-success'><strong>kagent and ksession is running</strong></div>";
        } catch (Exception e) {
            return "<div class='alert alert-error'><strong>" + e.toString() + "</strong></div>";
        }
    }

    @GET
    @Path("/stop")
    @Produces({ "text/html" })
    public String stop() {
        try {
            delegate.stopKnowledgeSession();
            delegate.stopKnowledgeAgent();
            return "<div class='alert alert-success'><strong>kagent and ksession stopped</strong></div>";
        } catch (Exception e) {
            return "<div class='alert alert-error'><strong>" + e.toString() + "</strong></div>";
        }
    }

    @GET
    @Path("/poll")
    @Produces({ "text/html" })
    public String poll() {
        return delegate.poll();
    }

}
