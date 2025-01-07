package i;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import m.TestSourceBoot.PulsarSinkMessage;

public abstract class MessageForTopic implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	List<String> topics ;
	
	public MessageForTopic() {
		topics=new ArrayList<>();
	}
	
	
	public List<String> getTopics() {
		return topics;
	}    
    
}
