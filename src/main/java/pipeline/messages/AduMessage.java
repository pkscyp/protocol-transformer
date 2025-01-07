package pipeline.messages;

import java.util.ArrayList;
import java.util.List;

public class AduMessage extends BaseMessage<AduMessage> implements WithSinkTopic{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	String payload;
	Integer maxAduInBundle;
	List<String> sinkTopics=new ArrayList<>();
	

	public AduMessage(BaseMessage<?> src) {
		src.copyTo(this);
		this.maxAduInBundle=0;
		this.payload="";
	}


	public String getPayload() {
		return payload;
	}


	public AduMessage setPayload(String payload) {
		this.payload = payload;
		return this;
	}


	public Integer getMaxAduInBundle() {
		return maxAduInBundle;
	}


	public AduMessage setMaxAduInBundle(Integer maxAduInBundle) {
		this.maxAduInBundle = maxAduInBundle;
		return this;
	}


	@Override
	public List<String> getSinkTopics() {
		// TODO Auto-generated method stub
		return sinkTopics;
	}
    
	public AduMessage addTopics(String ... topics) {
		for(String topic: topics) {
			this.sinkTopics.add(topic);
		}
		return this;
	}


	@Override
	public boolean isValid() {
		// TODO Auto-generated method stub
		return this.maxAduInBundle>0;
	}
	
}
