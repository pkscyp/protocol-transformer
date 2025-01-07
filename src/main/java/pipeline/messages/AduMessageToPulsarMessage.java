package pipeline.messages;

import java.io.Serializable;
import java.util.function.BiFunction;

import org.apache.pulsar.client.api.TypedMessageBuilder;


public class AduMessageToPulsarMessage implements  BiFunction<TypedMessageBuilder<String>,AduMessage,TypedMessageBuilder<String>>, Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public AduMessageToPulsarMessage() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public TypedMessageBuilder<String> apply(TypedMessageBuilder<String> ts, AduMessage u) {
		
		ts.key(u.getMessageKey());
		ts.eventTime(System.currentTimeMillis());
		ts.value(u.getPayload());
		
		return ts;
	}

}
