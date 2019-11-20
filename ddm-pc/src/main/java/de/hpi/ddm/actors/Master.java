package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.*;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Master extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public Iterator workersIterator;
	
	public static final String DEFAULT_NAME = "master";

	public static Props props(final ActorRef reader, final ActorRef collector) {
		return Props.create(Master.class, () -> new Master(reader, collector));
	}

	public Master(final ActorRef reader, final ActorRef collector) {
		this.reader = reader;
		this.collector = collector;
		this.workers = new ArrayList<>();
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data
	public static class StartMessage implements Serializable {
		private static final long serialVersionUID = -50374816448627600L;
	}
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BatchMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609598L;
		private List<String[]> lines;
	}

	@Data
	public static class RegistrationMessage implements Serializable {
		private static final long serialVersionUID = 3303081601659723997L;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class StartHintCrackingMessage implements Serializable {
		private static final long serialVersionUID = 3303091601658723997L;
		private char[] alphabet;
		private char[] droppableHintChars;
		// TODO get this value dynamically !!!
		private int hintLength = 10;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class FoundHintMessage implements Serializable {
		private static final long serialVersionUID = 3304091601658723997L;
		private String hint;
	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef reader;
	private final ActorRef collector;
	private final List<ActorRef> workers;

	private Map<String, String> cracked_hints_by_password;

	private boolean workersInitiated = false;

	private long startTime;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(StartMessage.class, this::handle)
				.match(BatchMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.match(RegistrationMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(StartMessage message) {
		this.startTime = System.currentTimeMillis();

		this.reader.tell(new Reader.ReadMessage(), this.self());
	}

	protected void handle(FoundHintMessage msg) {
		System.out.println("Yo, found a hint: " + msg.getHint());
	}
	
	protected void handle(BatchMessage message) {
		
		///////////////////////////////////////////////////////////////////////////////////////////////////////
		// The input file is read in batches for two reasons: /////////////////////////////////////////////////
		// 1. If we distribute the batches early, we might not need to hold the entire input data in memory. //
		// 2. If we process the batches early, we can achieve latency hiding. /////////////////////////////////
		// TODO: Implement the processing of the data for the concrete assignment. ////////////////////////////
		///////////////////////////////////////////////////////////////////////////////////////////////////////

		// initialize worker's workload
		if (!this.workersInitiated) {
			// TODO move to startMessageHandler
			char[] alphabet = message.getLines().get(0)[2].toCharArray();

			String[] workerCharAssignments = new String[workers.size()];
			Arrays.fill(workerCharAssignments, "");

			for(int i = 0; i < alphabet.length; i++){
				workerCharAssignments[i % workers.size()] += alphabet[i];
			}

			for (int i = 0; i < workers.size(); i++) {
				workers.get(i).tell(new StartHintCrackingMessage(alphabet, workerCharAssignments[i].toCharArray()), this.self());
			}
			this.workersInitiated = true;
		}

		// Store passwords in Master

		// Distribute all Hints to all worker

//
//		if (message.getLines().isEmpty()) {
//			this.collector.tell(new Collector.PrintMessage(), this.self());
//			this.terminate();
//			return;
//		}

	//	Iterator workersIterator = this.workers.iterator();
		for (String[] line : message.getLines()){
		//	if(!workersIterator.hasNext()){workersIterator = this.workers.iterator();}
		//	System.out.println(this.workers.size());
          //  ActorRef worker = (ActorRef) workersIterator.next();
		//	worker.tell(line,this.self());
		//	System.out.println(Arrays.toString(line));
		}
		
		this.collector.tell(new Collector.CollectMessage("Processed batch of size " + message.getLines().size()), this.self());
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}
	
	protected void terminate() {
		this.reader.tell(PoisonPill.getInstance(), ActorRef.noSender());
		this.collector.tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		for (ActorRef worker : this.workers) {
			this.context().unwatch(worker);
			worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
		}
		
		this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		long executionTime = System.currentTimeMillis() - this.startTime;
		this.log().info("Algorithm finished in {} ms", executionTime);
	}

	protected void handle(RegistrationMessage message) {
		this.context().watch(this.sender());
		this.workers.add(this.sender());
		this.log().info("Registered {}", this.sender());
	}
	
	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		this.workers.remove(message.getActor());
//		this.log().info("Unregistered {}", message.getActor());
	}
}
