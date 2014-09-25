package com.thisisnoble.javatest.impl;

import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import com.thisisnoble.javatest.Event;
import com.thisisnoble.javatest.Orchestrator;
import com.thisisnoble.javatest.Processor;
import com.thisisnoble.javatest.Publisher;

public class NobleOrchestrator implements Orchestrator {
	
	private Vector<Processor> processorList = null;
	private Publisher publisher =null;
	private  CompositeEvent compositeEvent =null;
	private static NobleOrchestrator orcherstrator =null;
	// used for thread safety
	private AtomicInteger counter = new AtomicInteger(0);

	private NobleOrchestrator() {
		processorList =new Vector<Processor>(1000);
	}

	@Override
	public void register(Processor processor) {
		// TODO Auto-generated method stub
		// adding the processor to the list
		processorList.add(processor);
	}

	@Override
	public void receive(Event event) {
		// TODO Auto-generated method stub
		// adding the event to the list and make it the parent
		if (compositeEvent == null){
		compositeEvent =new CompositeEvent(event.getId(), event);
		  for(Processor processor : processorList){
			  if (processor.interestedIn(event))
				  processor.process(event);
			      counter.incrementAndGet();
			      System.out.println("The events passed correctly to the processors");
		  }
		}
		// otherwise make the event a child
		else {
			synchronized (compositeEvent) {
				compositeEvent.addChild(event);
				if (compositeEvent.size() == counter.get()) {
				publisher.publish(compositeEvent);
				System.out.println("The events are already published");
				// empty the compositeEvent
				counter.set(0);
				compositeEvent = null;
				}
			}

		}
		
	}

	@Override
	public synchronized void setup(Publisher publisher) {
		// TODO Auto-generated method stub
			if (publisher == null)
				throw new IllegalArgumentException("Bad publisher");
			else
				 this.publisher = publisher;
	}
	
	public static NobleOrchestrator createOrchestrator(){
		if (orcherstrator ==null){
				   orcherstrator = new NobleOrchestrator();
		}
		return orcherstrator;
	}
	// helper method
	public int processorsCount(){
		// return the number of processors list
		if (processorList != null)
			return processorList.size();
		else
			return -1;
	}
	

}
