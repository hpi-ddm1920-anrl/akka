package de.hpi.ddm.actors;

import java.io.*;
import java.nio.ByteBuffer;

import akka.actor.*;
import akka.serialization.ByteBufferSerializer;
import akka.serialization.Serialization;
import akka.serialization.SerializationExtension;
import akka.serialization.Serializer;
import de.hpi.ddm.configuration.ConfigurationSingleton;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.serialization.ByteBufferSerializer;
import com.typesafe.config.ConfigFactory;

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


	    /*
	     @Override
        public void toBinary(Object o, ByteBuffer buf) {
            val pool = new akka.io.DirectByteBufferPool(ConfigurationSingleton.get().getDataSize() * 1000000, maxPoolEntries = 10);
            val buffer = pool.aquire();
            ObjectOutputStream objectOutputStreamStream = new ObjectOutputStream(o);
            objectOutputStreamStream.write(buffer);
            ObjectOutputStream.close();
        }

        @Override
        public Object fromBinary(ByteBuffer buf, String manifest) throws NotSerializableException {return null;}


	     */
		ActorRef receiver = message.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));
        //ActorSelection selection = this.context().actorSelection("akka://ddm@"+ip+":"+port+"user/receiver#"+receiver.path().uid());


/*
-----------
        // you need to know the maximum size in bytes of the serialized messages
        // Find the Serializer for it
        // Get the Serialization Extension

        ActorSystem system = this.context().actorFor("ddm");
        Serialization serialization = SerializationExtension.get(system);
        Serializer serializer = serialization.findSerializerFor(message);
        serializer.toBinary(message);

        //FOR RECEIVER: https://doc.akka.io/docs/akka/2.5.6/java/serialization.html
        // Turn it back into an object,
        // the nulls are for the class manifest and for the classloader
        String back = (String) serializer.fromBinary(bytes);
--------






        ByteBufferSerializer.toBinary​(message, buf);
        message.read
        pool.release(buf);
        message.writeObject(message);

        try {
            val pool = new akka.io.DirectByteBufferPool(ConfigurationSingleton.get().getDataSize() * 1000000, maxPoolEntries = 10);
            val buf = pool.acquire();
            ObjectInputStream objectInputStream = new ObjectInputStream(message);
             message.readObject(objectInputStream);
            objectInputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        //Objekt lesen
        LargeMessage object = LargeMessage objectInputStream.readObject();


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

        // Turn it into bytes
        byte[] bytes = serializer.toBinary(message.getMessage());
        this.log().info("Jetzt kommt Nachricht Bytes");

        this.log().info(serializer.toBinary(message.getMessage()).toString());
        this.log().info("To Binary: " + serializer.fromBinary(bytes));
        receiverProxy.tell(new BytesMessage<>(bytes, this.sender(), message.getReceiver()), this.self());



     //   receiverProxy.tell(bytes, this.self());
	}

	private void handle(BytesMessage<?> message) {
		// Reassemble the message content, deserialize it and/or load the content from some local location before forwarding its content.
        ActorSystem system = this.context().system();

        // Get the Serialization Extension
        Serialization serialization = SerializationExtension.get(system);
        // Find the Serializer for it


        Serializer serializer = serialization.findSerializerFor((byte []) message.getBytes());

        this.log().info((String) serializer.fromBinary((byte [])message.getBytes()));
        message.getReceiver().tell(serializer.fromBinary((byte []) message.getBytes()), message.getSender());

	}


}
