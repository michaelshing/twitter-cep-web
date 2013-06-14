package org.example;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.drools.ClockType;
import org.drools.KnowledgeBase;
import org.drools.KnowledgeBaseConfiguration;
import org.drools.KnowledgeBaseFactory;
import org.drools.agent.KnowledgeAgent;
import org.drools.agent.KnowledgeAgentConfiguration;
import org.drools.agent.KnowledgeAgentFactory;
import org.drools.agent.impl.PrintStreamSystemEventListener;
import org.drools.base.evaluators.AfterEvaluatorDefinition;
import org.drools.conf.EventProcessingOption;
import org.drools.definition.KnowledgePackage;
import org.drools.definition.rule.Rule;
import org.drools.event.knowledgeagent.AfterChangeSetAppliedEvent;
import org.drools.event.rule.DebugKnowledgeAgentEventListener;
import org.drools.event.rule.DefaultKnowledgeAgentEventListener;
import org.drools.io.ResourceChangeScannerConfiguration;
import org.drools.io.ResourceFactory;
import org.drools.runtime.KnowledgeSessionConfiguration;
import org.drools.runtime.StatefulKnowledgeSession;
import org.drools.runtime.conf.ClockTypeOption;
import org.drools.runtime.rule.WorkingMemoryEntryPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.Status;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

public class TwitterCEPDelegate {

    private static Logger logger = LoggerFactory.getLogger(TwitterCEPDelegate.class);

    static {
        // avoid classloading/serialization issue
        AfterEvaluatorDefinition after = new AfterEvaluatorDefinition();
    }

    private static TwitterCEPDelegate singleton = new TwitterCEPDelegate();

    private static int MAX_RESPONSE_SIZE = 20;

    private KnowledgeAgent kagent;
    private StatefulKnowledgeSession ksession;

    private TwitterStream twitterStream;
    private List<Status> tweetList = new ArrayList<Status>();

    private String ruleNamesInUse;
    private boolean kagentRunning = false;
    private boolean ksessionRunning = false;

    private TwitterCEPDelegate() {

    }

    public static TwitterCEPDelegate getInstance() {
        return singleton;
    }

    public synchronized String run() {

        if (ksessionRunning) {
            throw new RuntimeException("ksession is running!");
        }

        // Creates a knowledge session
        ksession = createKnowledgeSession();

        // store rule names in use
        StringBuilder sb = new StringBuilder();
        Collection<KnowledgePackage> knowledgePackages = ksession.getKnowledgeBase().getKnowledgePackages();
        for (KnowledgePackage knowledgePackage : knowledgePackages) {
            Collection<Rule> rules = knowledgePackage.getRules();
            for (Rule rule : rules) {
                sb.append(rule.getName() + ", ");
            }
        }
        if (sb.length() > 2) {
            sb.delete(sb.length() - 2, sb.length());
        }
        ruleNamesInUse = sb.toString();

        tweetList = new ArrayList<Status>();
        ksession.setGlobal("tweetList", tweetList);

        // Gets the stream entry point
        final WorkingMemoryEntryPoint ep = ksession.getWorkingMemoryEntryPoint("twitter");

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
        logger.info("ksession is running");

        return sb.toString();
    }

    public String poll() {
        logger.info("poll tweets!");

        StringBuffer buf = new StringBuffer();

        // display rules in use
        buf.append("<div class='alert alert-success'><strong>rules = " + ruleNamesInUse + "</strong></div>");

        int start = tweetList.size() < MAX_RESPONSE_SIZE ? 0 : tweetList.size() - MAX_RESPONSE_SIZE;
        List<Status> outList = new ArrayList<Status>(tweetList.subList(start, tweetList.size()));

        for (int i = 0; i < outList.size(); i++) {
            Status status = outList.get(i);
            buf.append("<div class='alert alert-info'><strong>" + status.getUser().getScreenName() + "</strong> "
                    + status.getText() + "</div>");
        }

        return buf.toString();
    }

    private StatefulKnowledgeSession createKnowledgeSession() {
        KnowledgeBase kbase = kagent.getKnowledgeBase();
        KnowledgeSessionConfiguration ksconf = KnowledgeBaseFactory.newKnowledgeSessionConfiguration();
        ksconf.setOption(ClockTypeOption.get(ClockType.REALTIME_CLOCK.getId()));
        StatefulKnowledgeSession ksession = kbase.newStatefulKnowledgeSession(ksconf, null);
        return ksession;
    }

    public synchronized void stopKnowledgeSession() {

        if (!ksessionRunning) {
            throw new RuntimeException("ksession is not running!");
        }

        twitterStream.cleanUp();
        ksession.halt();
        ksession.dispose();

        ksessionRunning = false;
        logger.info("ksession stopped");

    }

    public synchronized void startKnowledgeAgent() {

        if (kagentRunning) {
            throw new RuntimeException("kagent is running!");
        }

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
        sconf.setProperty("drools.resource.scanner.interval", "10");
        ResourceFactory.getResourceChangeScannerService().configure(sconf);
        ResourceFactory.getResourceChangeScannerService().setSystemEventListener(new PrintStreamSystemEventListener());
        ResourceFactory.getResourceChangeScannerService().start();
        ResourceFactory.getResourceChangeNotifierService().start();

        kagentRunning = true;
        logger.info("kagent started!");
    }

    public synchronized void stopKnowledgeAgent() {

        if (!kagentRunning) {
            throw new RuntimeException("kagent is not running!");
        }

        ResourceFactory.getResourceChangeScannerService().stop();
        ResourceFactory.getResourceChangeNotifierService().stop();
        kagent.monitorResourceChangeEvents(false);
        kagent.dispose();

        kagentRunning = false;
        kagent = null;
        logger.info("kagent stopped");
    }

    class HaltKnowledgeAgentEventListener extends DefaultKnowledgeAgentEventListener {
        public void afterChangeSetApplied(AfterChangeSetAppliedEvent event) {
            // If the changeset is updated, halt the ksession and re-run
            if (ksessionRunning) {
                try {
                    singleton.stopKnowledgeSession();
                    singleton.run();
                } catch (Exception e) {
                    // maybe fatal error
                    logger.error("restarting ksession failed", e);
                }
            }
        }
    }
}
