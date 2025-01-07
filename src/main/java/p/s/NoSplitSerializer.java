package p.s;

import java.io.IOException;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class NoSplitSerializer implements SimpleVersionedSerializer<NoSplit>  {

	private final ObjectMapper _mapper = new ObjectMapper();
	
	public NoSplitSerializer() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public NoSplit deserialize(int version, byte[] serialized) throws IOException {
		
		return _mapper.readValue(serialized, NoSplit.class);
	}

	@Override
	public int getVersion() {
		return 1;
	}

	@Override
	public byte[] serialize(NoSplit obj) throws IOException {
		String b = _mapper.writeValueAsString(obj);
		return b.getBytes();
	}

}
