package p.s;

import java.io.IOException;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import i.MessageForTopic;
import p.s.PulsarSinkWriter.PulsarSinkFactory;

public class PulsarSink<IN extends MessageForTopic> implements Sink<IN> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(PulsarSink.class);
	

    final PulsarSinkFactory<IN> factory;
	public PulsarSink(
			 PulsarSinkFactory<IN> factory
			) {
		this.factory = factory;
	}

	@Override
	public SinkWriter<IN> createWriter(WriterInitContext  context) throws IOException {
		return factory.createWriter(context);
	}

	@Override
	public SinkWriter<IN> createWriter(InitContext arg0) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	

}
