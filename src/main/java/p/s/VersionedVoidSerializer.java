package p.s;

import java.io.IOException;

import org.apache.flink.core.io.SimpleVersionedSerializer;

public class VersionedVoidSerializer implements SimpleVersionedSerializer<Void> {

	public VersionedVoidSerializer() {
		// TODO Auto-generated constructor stub
	}
    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(Void obj) throws IOException {
        return new byte[0];
    }

    @Override
    public Void deserialize(int version, byte[] serialized) throws IOException {
        return null;
    }
}
