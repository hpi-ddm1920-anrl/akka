package de.hpi.ddm.actors;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import de.hpi.ddm.MasterSystem;
import org.apache.commons.collections4.iterators.PermutationIterator;


public class Worker extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "worker";


	public static Props props() {
		return Props.create(Worker.class);
	}

	public Worker() {
		this.cluster = Cluster.get(this.context().system());
	}

	private HashSet<String> hintHashes;
	private char[] alphabet;
	private char[] assignedLetters;

	// Bilde alle Permutationen

	// JE LINE: 1. Bilde alle Permutationen aus PasswordChards für Länge PasswordLength - 1. Generiere für diese Permutationen sha wert bis hint 1-hint n gefunden.
	// 2. Berechne auszuschließende Chars aus Hints
	// 3. Generiere alle Permutationen für PasswordChars ohne auszuschließende Chars
	// 4. Generiere für alle Permutationen Sha Werte bis Passwort gefunden
	////////////////////
	// Actor Messages //
	////////////////////

	/////////////////
	// Actor State //
	/////////////////

	private Member masterSystem;
	private final Cluster cluster;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
		
		this.cluster.subscribe(this.self(), MemberUp.class, MemberRemoved.class);
	}

	@Override
	public void postStop() {
		this.cluster.unsubscribe(this.self());
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(CurrentClusterState.class, this::handle)
				.match(MemberUp.class, this::handle)
				.match(MemberRemoved.class, this::handle)
				.match(Master.StartHintCrackingMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(Master.StartHintCrackingMessage msg) {
		// start working
		this.log().info("Started Hint Cracking with Letters: " + String.valueOf(msg.getDroppableHintChars()));
		for (char c : msg.getDroppableHintChars()) {

			char[] alphabet = msg.getAlphabet();
			Collection<Character> localAlphabet = new HashSet<Character>();
			HashSet<String> hintHashes = msg.getHintHashes();

			for (char a : alphabet) {
				if (a != c) {
					localAlphabet.add(a);
				}
			}

			PermutationIterator<Character> permutations = new PermutationIterator<Character>(localAlphabet);
			int counter = 1;
			while (permutations.hasNext()) {
				if (counter % 1000000 == 0) {
					this.log().info("Tried " + counter + " Hint Hashes for Letter " + c);
				}
				String currentTry = Worker.charCollectionToString(permutations.next());
				if (hintHashes.contains(hash(currentTry))) {
					// Refactor not to do this in the recursion step but in parent function
					this.getContext().actorSelection(masterSystem.address() + "/user/" + Master.DEFAULT_NAME).tell(new Master.FoundHintMessage(currentTry), this.self());
					this.log().info("Found Hash Match " + currentTry);
				}

				counter++;
			}
		}
	}


	private void handle(CurrentClusterState message) {
		message.getMembers().forEach(member -> {
			if (member.status().equals(MemberStatus.up()))
				this.register(member);
		});

	}

	private void handle(MemberUp message) {
		this.register(message.member());
	}

	private void register(Member member) {
		if ((this.masterSystem == null) && member.hasRole(MasterSystem.MASTER_ROLE)) {
			this.masterSystem = member;
			
			this.getContext()
				.actorSelection(member.address() + "/user/" + Master.DEFAULT_NAME)
				.tell(new Master.RegistrationMessage(), this.self());
		}
	}
	
	private void handle(MemberRemoved message) {
		if (this.masterSystem.equals(message.member()))
			this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
	}
	
	private String hash(String line) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hashedBytes = digest.digest(String.valueOf(line).getBytes("UTF-8"));
			
			StringBuffer stringBuffer = new StringBuffer();
			for (int i = 0; i < hashedBytes.length; i++) {
				stringBuffer.append(Integer.toString((hashedBytes[i] & 0xff) + 0x100, 16).substring(1));
			}
			return stringBuffer.toString();
		}
		catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
			throw new RuntimeException(e.getMessage());
		}
	}

	public static String charCollectionToString(Collection<Character> charArray) {
		StringBuilder sb = new StringBuilder();
		for (Character ch : charArray) {
			sb.append(ch);
		}
		return sb.toString();
	}

}
