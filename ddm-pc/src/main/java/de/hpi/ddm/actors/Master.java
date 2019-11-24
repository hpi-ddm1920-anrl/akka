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


	public static final String DEFAULT_NAME = "master";
	private int passwordLength;

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

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class CrackedPassword implements Serializable {
		private static final long serialVersionUID = 3303090001658723997L;
		private String passwordHash;
		private String crackedHash;
	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef reader;
	private final ActorRef collector;
	private final List<ActorRef> workers;
	private final ArrayList<BatchMessage> buffer;

	private Map<String, Set<String>> passwordHashDirectory = new HashMap<>();
	private Map<String, Set<Character>> crackedHintsByPassword = new HashMap<>();
	private Map<String, String> passwordTable = new HashMap();


	private Set<Character> alphabet = new HashSet<>();

	private boolean workersInitiated = false;

	private long startTime;

	private int hintThreshold;

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
				.match(CrackedPassword.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(StartMessage message) {
		this.startTime = System.currentTimeMillis();

		this.reader.tell(new Reader.ReadMessage(), this.self());
	}

	protected void handle(CrackedPassword msg){
		crackedHintsByPassword.remove(msg.getPasswordHash());
		passwordTable.put(msg.getPasswordHash(),msg.getCrackedHash());

		log().info("Cracked Password "+passwordTable.size()+"/"+ passwordHashDirectory.size());
//		this.collector.tell(Collector.CollectMessage(msg.getCrackedHash()));

		if(passwordTable.size() >= passwordHashDirectory.size()){
			terminate();
		};
	}

	protected void handle(FoundHintMessage msg) {
		String password_hash = getPasswordHashByHintHash(msg.getHintHash());
		Set<Character> hintChars = msg.hint.chars().mapToObj(c -> (char) c).collect(Collectors.toSet());

		// Add excluded char to Password Hash
		Character exludedChar = Sets.difference(alphabet, hintChars).immutableCopy().asList().get(0);

		Set<Character> cracked_hints = crackedHintsByPassword.get(password_hash);
		if (cracked_hints != null) {
			// if null the password was already cracked
			cracked_hints.add(exludedChar);

			// Check if password cracking can start
			if (cracked_hints.size() >= hintThreshold) {
				workers.get(new Random().nextInt(this.workers.size())).tell(
						new StartPasswordCrackingMessage(password_hash, toCharArray(Sets.difference(alphabet, cracked_hints))),
						this.self()
				);
				crackedHintsByPassword.remove(password_hash);
			}
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

        if (this.workers.size() < 1) {
            this.buffer.add(message);
            this.collector.tell(new Collector.CollectMessage("Saved batch of size " + message.getLines().size() + "to buffer."), this.self());
            this.reader.tell(new Reader.ReadMessage(), this.self());

        } else {
            // initialize worker's workload
            if (!this.workersInitiated && message.getLines().size() > 0) {
            	String [] firstLine = message.getLines().get(0);
                // populate alphabet
				alphabet.addAll(firstLine[2].chars().mapToObj(c -> (char) c).collect(toSet()));

				// set Lengths and "Hyperparameters"
				passwordLength = Integer.valueOf(firstLine[3]);
				int hintCount = firstLine.length - 5;
				hintThreshold = Math.min(passwordLength - 3, hintCount - 2);

				log().info("The hintThreshold is "+hintThreshold);

                // Distribute Work for Hint Cracking
                String[] workerCharAssignments = new String[workers.size()];
                Arrays.fill(workerCharAssignments, "");

                Iterator<Character> it = alphabet.iterator();
                for (int i = 0; i < alphabet.size(); i++) {
                    workerCharAssignments[i % workers.size()] += it.next();
                }

                for (int i = 0; i < workers.size(); i++) {
                    workers.get(i).tell(new StartHintCrackingMessage(toCharArray(alphabet), workerCharAssignments[i].toCharArray(), passwordLength), this.self());
                }

                this.workersInitiated = true;
            }

			for (String[] line : message.getLines()) {
				String password_hash = line[4];
				Set<String> currentHintHashes = new HashSet<String>();
				Collections.addAll(currentHintHashes, Arrays.copyOfRange(line, 5, line.length));

				// Store current entry for later matching
				passwordHashDirectory.put(password_hash, currentHintHashes);

				for (ActorRef w : workers){
					w.tell(new MoreHintsIncomingMessage(currentHintHashes), this.self());
				}

				// generate data structure that holds the hint results
				crackedHintsByPassword.put(password_hash, new HashSet<Character>());
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
		for (Map.Entry<String, Set<String>> pw : passwordHashDirectory.entrySet()){
			if (pw.getValue().contains(hintHash)){
				return pw.getKey();
			}
		}
		return null;
	}
}
