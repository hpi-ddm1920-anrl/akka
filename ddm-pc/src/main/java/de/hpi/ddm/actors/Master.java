package de.hpi.ddm.actors;

import akka.actor.*;
import com.google.common.collect.
		Sets;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;

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
		this.buffer = new ArrayList<BatchMessage>();
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
		private int hintLength;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class MoreHintsIncomingMessage implements Serializable {
		private static final long serialVersionUID = 3303953601658723997L;
		private Set<String> hintHashes;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class StartPasswordCrackingMessage implements Serializable {
		private static final long serialVersionUID = 3303091601658723998L;
		private String passwordHash;
		private char[] alphabet;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class FoundHintMessage implements Serializable {
		private static final long serialVersionUID = 3304091601658723997L;
		private String hint;
		private String hintHash;
	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef reader;
	private final ActorRef collector;
	private final List<ActorRef> workers;
	private final ArrayList<BatchMessage> buffer;

	private Map<String, Set<String>> password_hash_directory = new HashMap<>();
	private Map<String, Set<Character>> cracked_hints_by_password = new HashMap<>();

	private Set<Character> alphabet = new HashSet<>();

	private boolean workersInitiated = false;

	private long startTime;

	// TODO set dynamically
	private int hintThreshold = 4;

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
				.match(FoundHintMessage.class, this::handle)
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
		String password_hash = getPasswordHashByHintHash(msg.getHintHash());
		Set<Character> hintChars = msg.hint.chars().mapToObj(c -> (char) c).collect(Collectors.toSet());

		// Add excluded char to Password Hash
		Character exludedChar = Sets.difference(alphabet, hintChars).immutableCopy().asList().get(0);

		Set<Character> cracked_hints = cracked_hints_by_password.get(password_hash);
		cracked_hints.add(exludedChar);

		// Check if password cracking can start
		if (cracked_hints.size() >= hintThreshold) {
			// TODO distrubute work evenly


			workers.get(0).tell(
					new StartPasswordCrackingMessage(password_hash, toCharArray(Sets.difference(alphabet, cracked_hints))),
					this.self()
			);
			// TODO abort currently running cracking attempt when new Hint becomes available
		}
	}

	private static char[] toCharArray(Set<Character> setView) {
		char[] result = new char[setView.size()];
		Iterator<Character> it = setView.iterator();
		for (int i = 0; i < result.length; i++){
			result[i] = it.next();
		}
		return result;
	}

	protected void handle(BatchMessage message) {

        ///////////////////////////////////////////////////////////////////////////////////////////////////////
        // The input file is read in batches for two reasons: /////////////////////////////////////////////////
        // 1. If we distribute the batches early, we might not need to hold the entire input data in memory. //
        // 2. If we process the batches early, we can achieve latency hiding. /////////////////////////////////
        // TODO: Implement the processing of the data for the concrete assignment. ////////////////////////////
        ///////////////////////////////////////////////////////////////////////////////////////////////////////
        if (this.workers.size() < 1) {
            this.buffer.add(message);
            this.collector.tell(new Collector.CollectMessage("Saved batch of size " + message.getLines().size() + "to buffer."), this.self());
            this.reader.tell(new Reader.ReadMessage(), this.self());

        } else {
            // initialize worker's workload
            //Todo from second batch message on, this method and tehrefore its workers are skipped. How to handle second batchmessage and so on?
            if (!this.workersInitiated) {
                // TODO move to startMessageHandler
                // TODO aktuell werden alte batchmessage in Buffer gespeichert und anschließend unseren datenstrukturen hinzugefügt. Klappt nicht mit verschiedenen alphabeten. Anpassen!

                // populate alphabet
				alphabet.addAll(message.getLines().get(0)[2].chars().mapToObj(c -> (char) c).collect(toSet()));

                // Distribute Work for Hint Cracking
                String[] workerCharAssignments = new String[workers.size()];
                Arrays.fill(workerCharAssignments, "");

                Iterator<Character> it = alphabet.iterator();
                for (int i = 0; i < alphabet.size(); i++) {
                    workerCharAssignments[i % workers.size()] += it.next();
                }

                for (int i = 0; i < workers.size(); i++) {
                    // TODO get Length of hints dynamically !!!
                    workers.get(i).tell(new StartHintCrackingMessage(toCharArray(alphabet), workerCharAssignments[i].toCharArray(), 10), this.self());
                }

                this.workersInitiated = true;
            }

			for (String[] line : message.getLines()) {
				String password_hash = line[4];
				Set<String> currentHintHashes = new HashSet<String>();
				Collections.addAll(currentHintHashes, Arrays.copyOfRange(line, 5, line.length));

				// Store current entry for later matching
				password_hash_directory.put(password_hash, currentHintHashes);

				for (ActorRef w : workers){
					w.tell(new MoreHintsIncomingMessage(currentHintHashes), this.self());
				}

				// generate data structure that holds the hint results
				cracked_hints_by_password.put(password_hash, new HashSet<Character>());
			}

//
//		if (message.getLines().isEmpty()) {
//			this.collector.tell(new Collector.PrintMessage(), this.self());
//			this.terminate();
//			return;
//		}

            //	Iterator workersIterator = this.workers.iterator();
            for (String[] line : message.getLines()) {
                //	if(!workersIterator.hasNext()){workersIterator = this.workers.iterator();}
                //	System.out.println(this.workers.size());
                //  ActorRef worker = (ActorRef) workersIterator.next();
                //	worker.tell(line,this.self());
                //	System.out.println(Arrays.toString(line));
            }
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
		this.log().info("Unregistered {}", message.getActor());
	}

	private String getPasswordHashByHintHash(String hintHash){
		for (Map.Entry<String, Set<String>> pw : password_hash_directory.entrySet()){
			if (pw.getValue().contains(hintHash)){
				return pw.getKey();
			}
		}
		return null;
	}
}
