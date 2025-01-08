package pipeline.messages;

public class PduBundle extends BaseMessage<PduBundle>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	String payload;
	
	
	public PduBundle() {
		
	}

	public String getPayload() {
		return payload;
	}

	public PduBundle setPayload(String payload) {
		this.payload = payload;
		return this;
	}

}
