package p.s;

import java.util.function.BiFunction;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarSource<T> implements Source<T, NoSplit, Void>,ResultTypeQueryable<T> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(PulsarSource.class);
	
	
	
	
	final PulsarSourceFactoryManager factory;
	
	public PulsarSource( 
			PulsarSourceFactoryManager<T> factory) {
		this.factory = factory;
	}

	@Override
	public SourceReader<T, NoSplit> createReader(SourceReaderContext readerContext) throws Exception {
		logger.info("create Reader called {} ",readerContext.getConfiguration());
		
		return factory.createReader(readerContext);//  
	}

	@Override
	public SplitEnumerator<NoSplit, Void> createEnumerator(SplitEnumeratorContext<NoSplit> enumContext)
			throws Exception {
	
		return factory.createEnumerator(enumContext);
	}

	@Override
	public Boundedness getBoundedness() {
		// TODO Auto-generated method stub CONTINUOUS_UNBOUNDED
		return Boundedness.CONTINUOUS_UNBOUNDED;
	}

	@Override
	public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
		logger.info("getEnumeratorCheckpointSerializer");
		return new VersionedVoidSerializer();
	}

	@Override
	public SimpleVersionedSerializer<NoSplit> getSplitSerializer() {
		logger.info("getSplitSerializer");
		return new NoSplitSerializer();
	}

	@Override
	public SplitEnumerator<NoSplit, Void> restoreEnumerator(SplitEnumeratorContext<NoSplit> enumContext,
			Void arg1) throws Exception {

		return null;
	}

	@Override
	public TypeInformation<T> getProducedType() {
		// TODO Auto-generated method stub
		return  factory.getTypeInformation();  
	}

	
	
}
