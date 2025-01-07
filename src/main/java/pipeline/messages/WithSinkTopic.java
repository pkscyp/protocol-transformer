package pipeline.messages;

import java.util.List;

public interface WithSinkTopic {

	public List<String> getSinkTopics();
	public boolean isValid();
}
