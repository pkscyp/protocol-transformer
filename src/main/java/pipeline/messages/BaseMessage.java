package pipeline.messages;

import java.io.Serializable;

public class BaseMessage<T> implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Integer readerId;
	String messageKey;
	Boolean ackStatus;
	String pulsarMessageId;
	String sourceTopic;
	
	public BaseMessage() {
		
	}

	public Integer getReaderId() {
		return readerId;
	}

	public T setReaderId(Integer readerId) {
		this.readerId = readerId;
		return (T)this;
	}

	public String getMessageKey() {
		return messageKey;
	}

	public T setMessageKey(String messageKey) {
		this.messageKey = messageKey;
		return (T)this;
	}

	public Boolean getAckStatus() {
		return ackStatus;
	}

	public T setAckStatus(Boolean ackStatus) {
		this.ackStatus = ackStatus;
		return (T)this;
	}

	public String getPulsarMessageId() {
		return pulsarMessageId;
	}

	public T setPulsarMessageId(String pulsarMessageId) {
		this.pulsarMessageId = pulsarMessageId;
		return (T)this;
	}

	public String getSourceTopic() {
		return sourceTopic;
	}

	public T setSourceTopic(String sourceTopic) {
		this.sourceTopic = sourceTopic;
		return (T)this;
	}

	public void copyTo(BaseMessage<?> to) {
		to.ackStatus = this.ackStatus;
		to.setAckStatus(this.ackStatus);
		to.setMessageKey(this.messageKey);
		to.setPulsarMessageId(this.pulsarMessageId);
		to.setReaderId(this.readerId);
		to.setSourceTopic(this.sourceTopic);
		
		   
	}

	
}
