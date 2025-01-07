package p.s;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarSourceFactoryManager<T> implements Serializable {

	
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private static final Logger logger = LoggerFactory.getLogger(PulsarSourceFactoryManager.class);
		
		
		final BiFunction<Message<String>,Integer, T> converter;
		final Class<T> _class;
		final UUID objId;
		private static AtomicInteger instanceId=new AtomicInteger(0);
		private static Map<String,Object> registry=new ConcurrentHashMap<>();
		private final PulsarSourceConfigBuilder conf;
		
		public PulsarSourceFactoryManager(BiFunction<Message<String>, Integer, T> converter, Class<T> _class, PulsarSourceConfigBuilder conf) {
			this.converter = converter;
			this._class = _class;
			this.objId = UUID.randomUUID();
			this.conf = conf;
		}
		
		public PulsarSourceEnumerator createEnumerator(SplitEnumeratorContext<NoSplit> enumContext) {
			return new PulsarSourceEnumerator(enumContext,this);
		}


		public SourceReader<T, NoSplit> createReader(SourceReaderContext readerContext) {
			return  new PulsarSourceReader<T>(readerContext,this);
		}

		public TypeInformation<T> getTypeInformation() {
			return TypeInformation.of(this._class);
		}

		public BiFunction<Message<String>, Integer, T> getConvertor() {
			return this.converter;
		}
		
		public Integer getNewObjectId() {
			Integer id = this.instanceId.incrementAndGet();
			return id;
		}

		public void addReader(Integer id,ReaderMessageAckable r) {
			String globalId = String.format("%s/reader/%d",objId.toString(),id );
			registry.put(globalId, r);
		}
		
		public  void removeReader(Integer id) {
			String globalId = String.format("%s/reader/%d",objId.toString(),id );
			registry.remove(globalId);
			
			
		}
		public  void ackMessage(Integer id,String mesgId, String srcTopic) {
			String globalId = String.format("%s/reader/%d",objId.toString(),id );
			if(registry.containsKey(globalId)) {
				ReaderMessageAckable ack = (ReaderMessageAckable)registry.get(globalId);
				ack.acknowledge(mesgId,srcTopic);
			}else {
				logger.info("NotFound AckHandler Reader {} Reader",id);
			}
		}

		public void removeEnumerator(Integer id) {
			String globalId = String.format("%s/enumerator/%d",objId.toString(),id );
			registry.remove(globalId);
		}
		public void addEnumerator(Integer id,PulsarSourceEnumerator e) {
			String globalId = String.format("%s/enumerator/%d",objId.toString(),id );
			registry.put(globalId, e);
		}

		public String getReaderGlobalId(Integer id) {
			return String.format("%s/reader/%d",objId.toString(),id );
		}
		public PulsarSourceConfigBuilder getConfiguration() {
			// TODO Auto-generated method stub
			return conf;
		}

}
