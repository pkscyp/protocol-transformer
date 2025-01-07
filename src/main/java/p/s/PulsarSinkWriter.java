package p.s;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.ProducerBase;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import i.MessageForTopic;

@SuppressWarnings("rawtypes")
public class PulsarSinkWriter<IN extends MessageForTopic > implements SinkWriter<IN>{

	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(PulsarSinkWriter.class);	
	static final AtomicInteger _instanceId = new AtomicInteger();
	final Configuration config;
	final WriterInitContext context;
	
	final BiFunction<TypedMessageBuilder<String>,IN,TypedMessageBuilder<String>> converter;
	Integer myId;
	PulsarClient client;
	ConcurrentHashMap<String,Producer> producers = new ConcurrentHashMap<>();
	
	public PulsarSinkWriter(Configuration config, 
			WriterInitContext context, 
			BiFunction<TypedMessageBuilder<String>,IN,TypedMessageBuilder<String>> converter) {
		this.myId = _instanceId.incrementAndGet();
		this.config = config;
		this.context = context;
		this.converter = converter;
	}

	@Override
	public void close() throws Exception {
		
		for(Producer p: producers.values()) {
			p.close();
		}
		client.close();
	}

	@Override
	public void flush(boolean arg0) throws IOException, InterruptedException {
		
		for(Producer<String> p: producers.values()) {
			p.flush();
		}
	}

	@Override
	public void write(IN element, Context context) throws IOException, InterruptedException {
		TypedMessageBuilderImpl<String> messageBuilder = new TypedMessageBuilderImpl(null,Schema.STRING);
		TypedMessageBuilderImpl prepMessage = (TypedMessageBuilderImpl) converter.apply(messageBuilder, element);
		Message<String> sendMessage = prepMessage.getMessage();
		
		for(String topic: element.getTopics()) {
			try {
				ProducerBase<String> producer = (ProducerBase<String>) this.getOrCreateProducer(topic);
				if(producer == null) {
					throw new FlinkRuntimeException(" Unable to create producer for "+topic);
				}
				producer.sendAsync(sendMessage)
				.handle((m,t)->{ 
					if(t!=null) {
						logger.error("SendError",t);
						
					}else {
						logger.info("Sending Message {}",m);
					}
					return m;
					}
				);
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
		
	}
	
	public void start() {
		String broker_url = config.getString("pulsar.broker_url",null);
		if(broker_url == null)
			throw new FlinkRuntimeException("pulsar.broker_url");
		try {
			client = PulsarClient.builder()
					.serviceUrl(broker_url)
					.listenerThreads(1)									
					.ioThreads(1)
					.build();
		} catch (PulsarClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public Producer getOrCreateProducer(String topic) throws Exception {
		return producers.computeIfAbsent(topic, (k) -> {
			try {
				return createProducer(topic);
			} catch (Exception e) {
				
			}
			return null;
		});
	}

	private Producer<String> createProducer(String topic) throws PulsarClientException {
		Producer<String> producer = client.newProducer(Schema.STRING)
									.topic(topic)
									.create();
		return producer;
	}

	public static final class PulsarSinkFactory<IN extends MessageForTopic> implements Serializable {
		private static final long serialVersionUID = 1L;
		final Configuration config;
		final BiFunction<TypedMessageBuilder<String>,IN,TypedMessageBuilder<String>> converter;
		public PulsarSinkFactory(Configuration config,
				BiFunction<TypedMessageBuilder<String>,IN,TypedMessageBuilder<String>> converter) {
			this.config = config;
			this.converter=converter;
		}

		public PulsarSinkWriter<IN> createWriter(WriterInitContext context){
			PulsarSinkWriter<IN> psw = new PulsarSinkWriter<IN>(config,context,converter);
			 psw.start();
			return psw;
			
		}
	}
}
