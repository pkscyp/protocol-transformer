package p.s;


public interface ReaderMessageAckable {

	
	public void acknowledge(String mid, String srcTopic);
}
