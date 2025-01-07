package m;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.function.BiFunction;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import i.*;
import p.s.*;
import u.FlinkSettings;


public class TestSourceBoot {

	private static final Logger logger = LoggerFactory.getLogger(TestSourceBoot.class);
	
	public TestSourceBoot() {
		
	}

	public static class PulsarMessageConverter implements BiFunction<Message<String>,Integer,PulsarMsg>,Serializable {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		
		@Override
		public PulsarMsg apply(Message<String> msg, Integer id) {
			String mid = Base64.getEncoder().encodeToString(msg.getMessageId().toByteArray());
			String fullId = String.format("%s@%s", mid,msg.getTopicName());
			return new PulsarMsg(id,fullId,msg.getKey()==null?id.toString():msg.getKey()).setPayload(msg.getValue());
		}
		
	}
	
	public static void main(String[] args) throws IOException {
		final Configuration config = FlinkSettings.createConfiguration();
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
		env.setMaxParallelism(3);
		env.setParallelism(3);
		PulsarSourceConfigBuilder pb = new PulsarSourceConfigBuilder();
		pb.set(config).topicNames("persistent://public/default/om500_bundle")
		.subscriptionName("PduBundleReader")
		.subscriptionMode("Durable");
		final PulsarSink<PulsarSinkMessage> sink = createSink(config);
		final PulsarSourceFactoryManager<PulsarMsg> factory = 
				                                           new PulsarSourceFactoryManager<PulsarMsg>(
				                                        		   new PulsarMessageConverter()
				                                        		   ,PulsarMsg.class,pb);
		
		final PulsarSource<PulsarMsg> source  = new PulsarSource<PulsarMsg>(factory);
		env.fromSource(source, WatermarkStrategy.noWatermarks(), "Pulsar Key Shared Reader")
		.map((p) -> { Thread.sleep(100); return p;})			
		.map((p) ->{
			factory.ackMessage(p.readerId, p.getMsg(),"");
			return p;
		})
		.map(p -> {
			return new PulsarSinkMessage(p.imei,p.getPayload())
					.addTopic("persistent://public/default/outtest")
					;
		})
		.sinkTo(sink);
		
		
		try {
			env.execute("Pulsar KeyShared Source");
		} catch (Exception e) {
			logger.error("E",e);
		}

	}
	
	public static class MessageTranslator implements  BiFunction<TypedMessageBuilder<String>,PulsarSinkMessage,TypedMessageBuilder<String>>, Serializable  {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public TypedMessageBuilder<String> apply(TypedMessageBuilder<String> tb, PulsarSinkMessage u) {
			
			tb.key(u.imei);
			tb.eventTime(System.currentTimeMillis());
			tb.value(u.payload);
			
			return tb;
		}
		
	}
	
	public static PulsarSink<PulsarSinkMessage>  createSink(Configuration config){
		PulsarSinkWriter.PulsarSinkFactory<PulsarSinkMessage> factory =
				new PulsarSinkWriter.PulsarSinkFactory<PulsarSinkMessage>(
						config, new MessageTranslator()
						);
		return new PulsarSink<PulsarSinkMessage>(factory);
	}
	
	public static class PulsarSinkMessage extends MessageForTopic {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		public String imei;
		public String payload;
		
		public PulsarSinkMessage() {
			super();
		}

		public PulsarSinkMessage( String imei, String payload) {
			super();
			this.imei = imei;
			this.payload = payload;
		}
		
		public PulsarSinkMessage addTopic(String topic) {
			super.getTopics().add(topic);
			return this;
		}
		
	}

	
}
