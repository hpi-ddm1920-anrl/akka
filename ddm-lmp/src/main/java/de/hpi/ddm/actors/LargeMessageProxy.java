package de.hpi.ddm.actors;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import akka.actor.*;
import akka.serialization.*;
import de.hpi.ddm.configuration.ConfigurationSingleton;
import it.unimi.dsi.fastutil.Hash;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.serialization.ByteBufferSerializer;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.collections.map.HashedMap;
import scala.util.Success;

import static java.lang.System.exit;

public class LargeMessageProxy extends AbstractLoggingActor{

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";

	private HashMap<Integer, HashMap<Integer, BytesMessage>> cache;

	public static Props props() {
		return Props.create(LargeMessageProxy.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class LargeMessage<T> implements Serializable{
		private static final long serialVersionUID = 2940665245810221108L;
		private T message;
		private ActorRef receiver;
    }


	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BytesMessage<T> implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private T bytes;
		private ActorRef sender;
		private ActorRef receiver;
		private String manifest;
		private int serialIdentifier;

		private int messageHash;
		private int chunkId;
		private int messageChunkAmount;
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(LargeMessage.class, this::handle)
				.match(BytesMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	public LargeMessageProxy() {
		this.cache = new HashMap<Integer, HashMap<Integer, BytesMessage>>();
	}

	private void handle(LargeMessage<?> message) {
		ActorRef receiver = message.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));


        ActorSystem system = this.context().system();

        // Get the Serialization Extension
        Serialization serialization = SerializationExtension.get(system);

        // Find the Serializer for it
        Serializer serializer = serialization.findSerializerFor(message.getMessage());
        int serializerIdentifier = serializer.identifier();
        String manifest = Serializers.manifestFor(serialization.findSerializerFor(message.getMessage()),message.getMessage());
        serializer.includeManifest();

        // Turn it into bytes
        byte[] bytes = serialization.serialize(message.getMessage()).get();
        int messageHash = Arrays.hashCode(bytes);
        this.log().info("Nachricht Output: " + message.getMessage());
        this.log().info(serializer.toBinary(message.getMessage()).toString());

		int chunkSizeBytes = 100000;  // 10 KB
		int messageChunkAmount = ((int) bytes.length / chunkSizeBytes) +1;

		for (int i=0; i < messageChunkAmount; i++) {
			this.log().info("Sent Chunck "+(i+1)+"/"+messageChunkAmount);
			receiverProxy.tell(
					new BytesMessage<>(
							Arrays.copyOfRange(bytes, i * chunkSizeBytes, Math.min((i+1)*chunkSizeBytes, bytes.length)),
							this.sender(), message.getReceiver(), manifest, serializerIdentifier, messageHash,
							i, messageChunkAmount
					), this.self()
			);
		}
	}

	private void handle(BytesMessage<?> message) {

		int messageHash = message.getMessageHash();

		// save current message to cache
		this.cache.putIfAbsent(messageHash, new HashMap<Integer, BytesMessage>());
		this.cache.get(messageHash).put(message.getChunkId(), message);
			log().info("Received chunk "+ this.cache.get(messageHash).size() +"/"+message.getMessageChunkAmount());

		// check if current ByteMessage completes large message
		if (this.cache.get(messageHash).size() >= message.getMessageChunkAmount()) {
			log().info("Chunk completed");
			// send all messages that are completed to parent
			Iterator cachedMessages = this.cache.get(messageHash).entrySet().iterator();
			ByteArrayOutputStream reassembledMessage = new ByteArrayOutputStream();

			while(cachedMessages.hasNext()){
				Map.Entry pair = (Map.Entry)cachedMessages.next();
				BytesMessage bmsg = (BytesMessage) pair.getValue();
				try {
					reassembledMessage.write((byte[]) bmsg.getBytes());
				} catch (java.io.IOException e) {
					this.log().error("MESSAGE TOO LARGE TO DESIERIALIZE");
					return;
				}
			}
			byte[] fullMessage = reassembledMessage.toByteArray();


			// Reassemble the message content, deserialize it and/or load the content from some local location before forwarding its content.
			ActorSystem system = this.context().system();
			Serialization serialization = SerializationExtension.get(system);
			Object msg = (Object) serialization.deserialize(fullMessage, message.getSerialIdentifier(), message.getManifest());
			message.getReceiver().tell(((Success) msg).value(), message.getSender());
            this.log().info("Successfull forwarded reassembled message: " + ((Success) msg).value());
			// Delete cached message chunks to indicate that we are done
			this.cache.remove(messageHash);
		}
	}


}
