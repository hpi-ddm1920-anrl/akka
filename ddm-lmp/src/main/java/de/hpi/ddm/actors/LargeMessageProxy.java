package de.hpi.ddm.actors;

import java.io.*;
import java.nio.ByteBuffer;

import akka.actor.*;
import akka.serialization.*;
import de.hpi.ddm.configuration.ConfigurationSingleton;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.serialization.ByteBufferSerializer;
import com.typesafe.config.ConfigFactory;
import scala.util.Success;

public class LargeMessageProxy extends AbstractLoggingActor{

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";
	
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
	}
	
	/////////////////
	// Actor State //
	/////////////////
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	////////////////////
	// Actor Behavior //
	////////////////////
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(LargeMessage.class, this::handle)
				.match(BytesMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(LargeMessage<?> message) {



		ActorRef receiver = message.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));
        //ActorSelection selection = this.context().actorSelection("akka://ddm@"+ip+":"+port+"user/receiver#"+receiver.path().uid());


/*

        // This will definitely fail in a distributed setting if the serialized message is large!
		// Solution options:
		// 1. Serialize the object and send its bytes batch-wise (make sure to use artery's side channel then).
		// 2. Serialize the object and send its bytes via Akka streaming.
		// 3. Send the object via Akka's http client-server component.
		// 4. Other ideas ...
		*/



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
        //this.log().info("Jetzt kommt Nachricht Bytes");
        this.log().info("Nachricht Output: " + message.getMessage());
        this.log().info(serializer.toBinary(message.getMessage()).toString());
        //this.log().info("To Binary: " + serializer.fromBinary(bytes));
        receiverProxy.tell(new BytesMessage<>(bytes, this.sender(), message.getReceiver(), manifest, serializerIdentifier), this.self());



     //   receiverProxy.tell(bytes, this.self());
	}

	private void handle(BytesMessage<?> message) {
		// Reassemble the message content, deserialize it and/or load the content from some local location before forwarding its content.
        ActorSystem system = this.context().system();

        // Get the Serialization Extension
        Serialization serialization = SerializationExtension.get(system);
        // Find the Serializer for it

       // Serializer serializer = serialization.findSerializerFor(message.getBytes());
        Object msg = (Object) serialization.deserialize((byte[])message.getBytes(),message.getSerialIdentifier(), message.getManifest());
        message.getReceiver().tell(((Success) msg).value(), message.getSender());




	}


}
