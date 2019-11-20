package de.hpi.ddm.actors;

import java.io.Serializable;
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
import lombok.Data;
import scala.compat.java8.MakesSequentialStream;
import sun.nio.cs.Surrogate;

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
				.match(String[].class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(Master.StartHintCrackingMessage msg){
		// start working
		for (char c : msg.getDroppableHintChars()) {
			char[] localAlphabet = (new ArrayList<Character>(msg.getAlphabet()).remove(c)).toArray();
			heapPermutation(localAlphabet, localAlphabet.length, msg.getHintLength());
		}
	}

	private void handle(String[] message) {

		// TO-Do Permutationen mit hashes irgendwo zentral erzeugen (auf jeden fall nicht jedes mal neu hier?)
		// z.B. https://stackoverflow.com/questions/8717375/how-to-effectively-store-a-large-set-of-permutations
		Set<Character> passwordChars = new HashSet();
		for(char c : message[2].toCharArray()) {passwordChars.add(c);}
		int passwordLength = Integer.valueOf(message[3]);
		String passwordHash = message[4];
		int amountOfHints = message.length - 5;
		int foundHints = amountOfHints;
		String password = "password cracking did not work";

		// Generate all permutations for hints based on password chars (length -1)
		ArrayList<String> hintPermutations = new ArrayList();

		//In der folgenden Zeile gibt es einen GC overhead limit exceeded (outofmemoryerror)
		//--> Es muss mit den Permutationen irgendwie effizienter umgegangen werden
		heapPermutation(message[2].toCharArray(),message[2].toCharArray().length,message[2].toCharArray().length-1, hintPermutations);
		System.out.println(hintPermutations);

		for (String permutation: hintPermutations) {
			if(foundHints == amountOfHints){break;}
			String hash = hash(permutation);
			// Check if any hashed permutation is a hint (5 to amount of hints)
			for(int i = 5; i < message.length; i++) {

				// if hint is found, apply hint
				if(hash.equalsIgnoreCase(message[i])){
					foundHints ++;
				for(char c : passwordChars){
					// Remove char that does not appear in hint from passwordchars
					if(permutation.indexOf(c) < 0){passwordChars.remove(c);}}
				}
			}
		}

		// After applying all hints, generate permutation for remaining password chars and test password
		ArrayList<String> passwordPermutations = new ArrayList<>();
		heapPermutation(passwordChars.toString().toCharArray(),passwordChars.size(), passwordLength, passwordPermutations);
		// Generate hash for each passwordPermutation and check if it is our desired password
		for(String permutation: passwordPermutations){
			String hash = hash(permutation);
			if(passwordHash.equalsIgnoreCase(hash)){password = permutation; break;}
		}

		System.out.println(password);
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

	// Generating all permutations of an array using Heap's Algorithm
	// https://en.wikipedia.org/wiki/Heap's_algorithm
	// https://www.geeksforgeeks.org/heaps-algorithm-for-generating-permutations/
	private void heapPermutation(char[] a, int size, int n) {
		// If size is 1, store the obtained permutation
		if (size == 1 && hintHashes.contains(hash(String.valueOf(a)))) {
			// Refactor not to do this in the recursion step but in parent function
			this.getContext()
					.actorSelection(masterSystem.address() + "/user/" + Master.DEFAULT_NAME)
					.tell(new Master.FoundHintMessage(String.valueOf(a)), this.self());
		}

		for (int i = 0; i < size; i++) {
			heapPermutation(a, size - 1, n, l);

			// If size is odd, swap first and last element
			if (size % 2 == 1) {
				char temp = a[0];
				a[0] = a[size - 1];
				a[size - 1] = temp;
			}

			// If size is even, swap i-th and last element
			else {
				char temp = a[i];
				a[i] = a[size - 1];
				a[size - 1] = temp;
			}
		}
	}

}