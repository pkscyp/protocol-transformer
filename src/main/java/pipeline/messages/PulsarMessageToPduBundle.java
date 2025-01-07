package pipeline.messages;

import java.io.Serializable;
import java.util.Base64;
import java.util.function.BiFunction;

import org.apache.pulsar.client.api.Message;



public class PulsarMessageToPduBundle implements BiFunction<Message<String>,Integer,PduBundle>,Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public PulsarMessageToPduBundle() {
		
	}

	@Override
	public PduBundle apply(Message<String> t, Integer u) {
		PduBundle pb = new PduBundle()
				.setMessageKey(t.getKey())
				.setPulsarMessageId(Base64.getEncoder().encodeToString(t.getMessageId().toByteArray()))
				.setSourceTopic(t.getTopicName())
				.setReaderId(u)
				.setPayload(t.getValue());
		return pb;
	}

}
