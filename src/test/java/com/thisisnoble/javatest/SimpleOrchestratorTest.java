package com.thisisnoble.javatest;

import com.thisisnoble.javatest.events.MarginEvent;
import com.thisisnoble.javatest.events.RiskEvent;
import com.thisisnoble.javatest.events.ShippingEvent;
import com.thisisnoble.javatest.events.TradeEvent;
import com.thisisnoble.javatest.impl.CompositeEvent;
import com.thisisnoble.javatest.impl.NobleOrchestrator;
import com.thisisnoble.javatest.processors.MarginProcessor;
import com.thisisnoble.javatest.processors.RiskProcessor;
import com.thisisnoble.javatest.processors.ShippingProcessor;
import com.thisisnoble.javatest.util.TestIdGenerator;

import org.junit.Test;

import static com.thisisnoble.javatest.util.TestIdGenerator.tradeEventId;
import static org.junit.Assert.*;

public class SimpleOrchestratorTest {

    @Test
    public void tradeEventShouldTriggerAllProcessors() {
        TestPublisher testPublisher = new TestPublisher();
        Orchestrator orchestrator = setupOrchestrator();
        orchestrator.setup(testPublisher);
        TradeEvent te = new TradeEvent(tradeEventId(), 1000.0);
        orchestrator.receive(te);
        safeSleep(100);
        CompositeEvent ce = (CompositeEvent) testPublisher.getLastEvent(); 
        assertEquals(4, ce.size());
        assertEquals(te, ce.getParent());
        
        if (ce.getChildById("tradeEvt-riskEvt") instanceof RiskEvent){
        	RiskEvent re1 = ce.getChildById("tradeEvt-riskEvt");
            assertNotNull(re1);
            assertEquals(50.0, re1.getRiskValue(), 0.01);
        }
        else if (ce.getChildById("tradeEvt-marginEvt") instanceof MarginEvent){
            MarginEvent me1 = ce.getChildById("tradeEvt-marginEvt");
            assertNotNull(me1);
            assertEquals(10.0, me1.getMargin(), 0.01);
        }
        else if (ce.getChildById("tradeEvt-shipEvt") instanceof TradeEvent){
            TradeEvent te1 = ce.getChildById("tradeEvt-shipEvt");
            assertNotNull(te1);
            assertEquals(10.0, te1.getNotional(), 0.01);
        }
        else if (ce.getChildById("tradeEvt-shipEvt-riskEvt") instanceof ShippingEvent){
            ShippingEvent se2 = ce.getChildById("tradeEvt-shipEvt-riskEvt");
            assertNotNull(se2);
            assertEquals(10.0, se2.getShippingCost(), 0.01);
        }
    }

    @Test
    public void shippingEventShouldTriggerOnly2Processors() {
        TestPublisher testPublisher = new TestPublisher();
        Orchestrator orchestrator = setupOrchestrator();
        orchestrator.setup(testPublisher);
        ShippingEvent se = new ShippingEvent("ship2", 500.0);
        orchestrator.receive(se);
        safeSleep(100);
        CompositeEvent ce = (CompositeEvent) testPublisher.getLastEvent();
        assertEquals(se, ce.getParent());
        assertEquals(3, ce.size());
        if (ce.getChildById("ship2") instanceof ShippingEvent){
        ShippingEvent re2 = ce.getChildById("ship2");
        assertNotNull(re2);
        assertEquals(500.0, re2.getShippingCost(), 0.01);
        }
        else if (ce.getChildById("ship2") instanceof MarginProcessor){
        MarginEvent me2 = ce.getChildById("ship2-marginEvt");
        assertNotNull(me2);
        assertEquals(5.0, me2.getMargin(), 0.01);
        }
    }

    private Orchestrator setupOrchestrator() {
        Orchestrator orchestrator = createOrchestrator();
        orchestrator.register(new RiskProcessor(orchestrator));
        orchestrator.register(new MarginProcessor(orchestrator));
        orchestrator.register(new ShippingProcessor(orchestrator));
        return orchestrator;
    }

    private void safeSleep(long l) {
        try {
            Thread.sleep(l);
        } catch (InterruptedException e) {
            //ignore
        }
    }

    private Orchestrator createOrchestrator() {
        return NobleOrchestrator.createOrchestrator();
    }
    
    public static void main(String[] args) {
    	SimpleOrchestratorTest simpleOrchestratorTest = new SimpleOrchestratorTest();
    	simpleOrchestratorTest.shippingEventShouldTriggerOnly2Processors();
    	simpleOrchestratorTest.tradeEventShouldTriggerAllProcessors();
    	
    	
	}
}
