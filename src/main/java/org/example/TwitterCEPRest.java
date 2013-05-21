package org.example;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import org.drools.ClockType;
import org.drools.KnowledgeBase;
import org.drools.KnowledgeBaseConfiguration;
import org.drools.KnowledgeBaseFactory;
import org.drools.agent.KnowledgeAgent;
import org.drools.agent.KnowledgeAgentConfiguration;
import org.drools.agent.KnowledgeAgentFactory;
import org.drools.agent.impl.PrintStreamSystemEventListener;
import org.drools.conf.EventProcessingOption;
import org.drools.event.knowledgeagent.AfterChangeSetAppliedEvent;
import org.drools.event.rule.DebugKnowledgeAgentEventListener;
import org.drools.event.rule.DefaultKnowledgeAgentEventListener;
import org.drools.io.ResourceChangeScannerConfiguration;
import org.drools.io.ResourceFactory;
import org.drools.runtime.KnowledgeSessionConfiguration;
import org.drools.runtime.StatefulKnowledgeSession;
import org.drools.runtime.conf.ClockTypeOption;
import org.drools.runtime.rule.WorkingMemoryEntryPoint;

import twitter4j.Status;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

@Path("/")
public class TwitterCEPRest {

    private static int MAX_RESPONSE_SIZE = 20;

    private static KnowledgeAgent kagent;
    private static StatefulKnowledgeSession ksession;
    private static TwitterStream twitterStream;
    private static List<Status> tweetList = new ArrayList<Status>();
    private static boolean kagentRunning = false;
    private static boolean ksessionRunning = false;

    @GET
    @Path("/start")
    @Produces({ "text/html" })
    public String start() {
        startKnowledgeAgent();
        run();
        return "kagent and ksession is running";
    }
    
    @GET
    @Path("/refresh")
    @Produces({ "text/html" })
    public String refresh() {
        stopKnowledgeSession();
        run();
        return "A new ksession is created and running";
    }
    
    @GET
    @Path("/stop")
    @Produces({ "text/html" })
    public String stop() {
        StringBuilder sb = new StringBuilder();
        String result = pollTweet();
        sb.append("kagent and ksession stopped<br>");
        sb.append("-------------------------<br>");
        sb.append(result);
        stopKnowledgeSession();
        stopKnowledgeAgent();
        return sb.toString();
    }
    
    @GET
    @Path("/poll")
    @Produces({ "text/html" })
    public String poll() {
        return pollTweet();
    }
    
    private String pollTweet() {
        System.out.println("pollTweet!");

        StringBuffer buf = new StringBuffer();
        int start = tweetList.size() < MAX_RESPONSE_SIZE ? 0 : tweetList.size()
                - MAX_RESPONSE_SIZE;
        List<Status> outList = new ArrayList<Status>(tweetList.subList(start,
                tweetList.size()));

        for (int i = 0; i < outList.size(); i++) {
            Status status = outList.get(i);
            buf.append("<span id='screenName'>"
                    + status.getUser().getScreenName() + "</span> "
                    + status.getText() + "<br>");
        }

        return buf.toString();
    }
    
    private static void run() {

        if (ksessionRunning) {
            throw new RuntimeException("ksession is running!");
        }

        // Creates a knowledge session
        ksession = createKnowledgeSession();

        tweetList = new ArrayList<Status>();
        ksession.setGlobal("tweetList", tweetList);

        // Gets the stream entry point
        final WorkingMemoryEntryPoint ep = ksession
                .getWorkingMemoryEntryPoint("twitter");

        // Connects to the twitter stream and register the listener
        StatusListener listener = new TwitterStatusListener(ep);
        twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(listener);
        twitterStream.sample();

        // Start a Thread to fire rules in Drools Fusion
        new Thread(new Runnable() {

            public void run() {
                ksession.fireUntilHalt();
            }
        }).start();

        ksessionRunning = true;
        System.out.println("ksession is running");
    }

    private static StatefulKnowledgeSession createKnowledgeSession() {
        KnowledgeBase kbase = kagent.getKnowledgeBase();
        System.out.println("createKnowledgeSession : kbase = " + kbase);
        KnowledgeSessionConfiguration ksconf = KnowledgeBaseFactory.newKnowledgeSessionConfiguration();
        ksconf.setOption(ClockTypeOption.get(ClockType.REALTIME_CLOCK.getId()));
        StatefulKnowledgeSession ksession = kbase.newStatefulKnowledgeSession(ksconf, null);
        return ksession;
    }
    
    private static void stopKnowledgeSession() {
        if (!ksessionRunning) {
            throw new RuntimeException("ksession is not running!");
        }

        twitterStream.cleanUp();
        ksession.halt();
        ksession.dispose();
        tweetList = new ArrayList<Status>();

        ksessionRunning = false;
        System.out.println("ksession stopped");

    }

    private static void startKnowledgeAgent() {

        if (kagent == null) {
            // prepare kbase to convey kbase configuration
            KnowledgeBaseConfiguration conf = KnowledgeBaseFactory.newKnowledgeBaseConfiguration();
            conf.setOption(EventProcessingOption.STREAM);
            KnowledgeBase kbase = KnowledgeBaseFactory.newKnowledgeBase(conf);

            // create kagent with change set
            KnowledgeAgentConfiguration aconf = KnowledgeAgentFactory.newKnowledgeAgentConfiguration();
            kagent = KnowledgeAgentFactory.newKnowledgeAgent("MyAgent", kbase, aconf);
            kagent.addEventListener(new DebugKnowledgeAgentEventListener());
            kagent.addEventListener(new HaltKnowledgeAgentEventListener());

            kagent.applyChangeSet(ResourceFactory.newClassPathResource("ChangeSet.xml"));

            // start ResourceChangeScanner
            ResourceChangeScannerConfiguration sconf = ResourceFactory.getResourceChangeScannerService()
                    .newResourceChangeScannerConfiguration();
            sconf.setProperty("drools.resource.scanner.interval", "20");
            ResourceFactory.getResourceChangeScannerService().configure(sconf);
            ResourceFactory.getResourceChangeScannerService().setSystemEventListener(
                    new PrintStreamSystemEventListener());
            ResourceFactory.getResourceChangeScannerService().start();
            ResourceFactory.getResourceChangeNotifierService().start();

            kagentRunning = true;
            System.out.println("kagent started!");
        } else {
            System.out.println("kagent is running!");
        }
    }
    
    private static void stopKnowledgeAgent() {

        ResourceFactory.getResourceChangeScannerService().stop();
        ResourceFactory.getResourceChangeNotifierService().stop();
        kagent.monitorResourceChangeEvents(false);

        kagentRunning = false;
        kagent = null;
    }

    static class HaltKnowledgeAgentEventListener extends DefaultKnowledgeAgentEventListener {
        public void afterChangeSetApplied(AfterChangeSetAppliedEvent event) {
            // If the changeset is updated, halt the ksession and re-run
            if (ksessionRunning) {
                try {
                    stopKnowledgeSession();
                    run();
                } catch (Exception e) {
                    // maybe fatal error
                    e.printStackTrace();
                }
            }
        }
    }
}
