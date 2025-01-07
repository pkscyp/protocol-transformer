package f;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.ProducerBase;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import p.s.PulsarSourceConfigBuilder;
import pipeline.messages.WithSinkTopic;

public class FnPushToPulsar<IN extends WithSinkTopic> extends RichMapFunction<IN,IN>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(FnPushToPulsar.class);
	
	final BiFunction<TypedMessageBuilder<String>,IN,TypedMessageBuilder<String>> converter;
	final PulsarSourceConfigBuilder config;
	Integer myId;
	PulsarClient client;
	ConcurrentHashMap<String,Producer<String>> producers = new ConcurrentHashMap<>();

	public FnPushToPulsar(BiFunction<TypedMessageBuilder<String>, IN, TypedMessageBuilder<String>> converter,
			PulsarSourceConfigBuilder config) {
		this.converter = converter;
		this.config = config;
		
	}

	@Override
	public IN map(IN element) throws Exception {
		if(!element.isValid()) return element;
		TypedMessageBuilderImpl<String> messageBuilder = new TypedMessageBuilderImpl<String>(null,Schema.STRING);
		TypedMessageBuilderImpl<String> prepMessage = (TypedMessageBuilderImpl<String>) converter.apply(messageBuilder, element);
		Message<String> sendMessage = prepMessage.getMessage();
		for(String topic: element.getSinkTopics()) {
			try {
				ProducerBase<String> producer = (ProducerBase<String>) this.getOrCreateProducer(topic);
				if(producer == null) {
					throw new FlinkRuntimeException(" Unable to create producer for "+topic);
				}
				producer.sendAsync(sendMessage)
				.handle((m,t)->{ 
					if(t!=null) {
						logger.error("SendError",t);						
					}
					return m;
					}
				);
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		return element;
	}
	
    @Override
    public void open(OpenContext openContext) throws Exception {
    	String broker_url = config.get(config.PULSAR_SERVICE_URL);
		if(broker_url == null)
			throw new FlinkRuntimeException("pulsar.broker_url");
		try {
			client = PulsarClient.builder()
					.serviceUrl(broker_url)
					.listenerThreads(config.get(config.PULSAR_NUM_LISTENER_THREADS))									
					.ioThreads(config.get(config.PULSAR_NUM_IO_THREADS))
					.build();
		} catch (PulsarClientException e) {
			throw new FlinkRuntimeException(e.getMessage());
		}
    }

    public Producer<String> getOrCreateProducer(String topic) throws Exception {
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
    @Override
    public void close() throws Exception {
    	for(Producer<String> p: producers.values()) {
			p.close();
		}
		client.close();
    }

}
