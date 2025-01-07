package p.s;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;

public class PulsarSourceConfigBuilder implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	final Configuration config;
	
	
	
	public PulsarSourceConfigBuilder() {
		this.config = new Configuration();
		// TODO Auto-generated constructor stub
	}
	
	public PulsarSourceConfigBuilder set(Configuration configuration) {
        Map<String, String> existedConfigs = config.toMap();
        List<String> duplicatedKeys = new ArrayList<>();
        for (Map.Entry<String, String> entry : configuration.toMap().entrySet()) {
            String key = entry.getKey();
            if (existedConfigs.containsKey(key)) {
                String value2 = existedConfigs.get(key);
                if (!Objects.equals(value2, entry.getValue())) {
                    duplicatedKeys.add(key);
                }
            }
        }
        config.addAll(configuration);
        return this;
    }
	
	
	
	public <T> T get(ConfigOption<T> key) {
		if(config.contains(key))
          return config.get(key);
		else
		  return key.defaultValue();
    }
	


	public static final String CLIENT_CONFIG_PREFIX="pulsar.";
	public static final String SOURCE_CONFIG_PREFIX="pulsar.source.";
	public static final String CONSUMER_CONFIG_PREFIX="pulsar.source.consumer";
	public static final String PRODUCER_CONFIG_PREFIX="pulsar.source.producer";
	
	public static final ConfigOption<String> PULSAR_SERVICE_URL =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX+"brokerUrl")
            .stringType()
            .noDefaultValue();
	public PulsarSourceConfigBuilder brokerUrl(String url) {
		config.set(PULSAR_SERVICE_URL, url);
		return this;
	}
	
	public static final ConfigOption<String> PULSAR_TOPIC_NAMES =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "topicNames")
                    .stringType()
                    .noDefaultValue();
	public PulsarSourceConfigBuilder topicNames(String name) {
		config.set(PULSAR_TOPIC_NAMES, name);
		return this;
	}
 
	public static final ConfigOption<Long> PULSAR_STATS_INTERVAL_SECONDS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "statsIntervalSeconds")
                    .longType()
                    .defaultValue(60L);
	public PulsarSourceConfigBuilder statsIntervalSeconds(long name) {
		config.set(PULSAR_STATS_INTERVAL_SECONDS, name);
		return this;
	}
	public static final ConfigOption<Integer> PULSAR_NUM_IO_THREADS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "numIoThreads")
                    .intType()
                    .defaultValue(1);
	public PulsarSourceConfigBuilder numIoThreads(int name) {
		config.set(PULSAR_NUM_IO_THREADS, name);
		return this;
	}
	
	 public static final ConfigOption<Integer> PULSAR_NUM_LISTENER_THREADS =
	            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "numListenerThreads")
	                    .intType()
	                    .defaultValue(1);
	 public PulsarSourceConfigBuilder numListenerThreads(int name) {
			config.set(PULSAR_NUM_LISTENER_THREADS, name);
			return this;
		}
	 public static final ConfigOption<Integer> PULSAR_CONNECTIONS_PER_BROKER =
	            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "connectionsPerBroker")
	                    .intType()
	                    .defaultValue(1);
	 public PulsarSourceConfigBuilder connectionsPerBroker(int name) {
			config.set(PULSAR_CONNECTIONS_PER_BROKER, name);
			return this;
		}
	 public static final ConfigOption<Boolean> PULSAR_USE_TCP_NO_DELAY =
	            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "useTcpNoDelay")
	                    .booleanType()
	                    .defaultValue(true);
	 public PulsarSourceConfigBuilder useTcpNoDelay(boolean name) {
			config.set(PULSAR_USE_TCP_NO_DELAY, name);
			return this;
		}
	 public static final ConfigOption<Integer> PULSAR_FETCH_ONE_MESSAGE_TIME =
	            ConfigOptions.key(SOURCE_CONFIG_PREFIX + "fetchOneMessageTime")
	                    .intType()
	                    .defaultValue(10);
	 public PulsarSourceConfigBuilder fetchOneMessageTime(int name) {
			config.set(PULSAR_FETCH_ONE_MESSAGE_TIME, name);
			return this;
		}
	 
	 public static final ConfigOption<Integer> PULSAR_MAX_FETCH_RECORDS =
	            ConfigOptions.key(SOURCE_CONFIG_PREFIX + "maxFetchRecords")
	                    .intType()
	                    .defaultValue(100);
	 public PulsarSourceConfigBuilder maxFetchRecords(int name) {
			config.set(PULSAR_MAX_FETCH_RECORDS, name);
			return this;
		}
	 public static final ConfigOption<String> PULSAR_SUBSCRIPTION_NAME =
	            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "subscriptionName")
	                    .stringType()
	                    .noDefaultValue();
	 public PulsarSourceConfigBuilder subscriptionName(String name) {
			config.set(PULSAR_SUBSCRIPTION_NAME, name);
			return this;
		}
	 public static final ConfigOption<SubscriptionMode> PULSAR_SUBSCRIPTION_MODE =
	            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "subscriptionMode")
	                    .enumType(SubscriptionMode.class)
	                    .defaultValue(SubscriptionMode.Durable);
	 public PulsarSourceConfigBuilder subscriptionMode(String name) {
			config.set(PULSAR_SUBSCRIPTION_MODE, SubscriptionMode.valueOf(name));
			return this;
		}
	 public static final ConfigOption<SubscriptionType> PULSAR_SUBSCRIPTION_TYPE =
	            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "subscriptionType")
	                    .enumType(SubscriptionType.class)
	                    .defaultValue(SubscriptionType.Key_Shared);
	 public PulsarSourceConfigBuilder subscriptionType(String name) {
			config.set(PULSAR_SUBSCRIPTION_TYPE, SubscriptionType.valueOf(name));
			return this;
		}
	 public static final ConfigOption<Integer> PULSAR_RECEIVER_QUEUE_SIZE =
	            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "receiverQueueSize")
	                    .intType()
	                    .defaultValue(1000);
	 public PulsarSourceConfigBuilder receiverQueueSize(int name) {
			config.set(PULSAR_RECEIVER_QUEUE_SIZE, name);
			return this;
		}
	 public static final ConfigOption<Long> PULSAR_NEGATIVE_ACK_REDELIVERY_DELAY_MICROS =
	            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "negativeAckRedeliveryDelayMicros")
	                    .longType()
	                    .defaultValue(TimeUnit.MINUTES.toMicros(1));
	 public PulsarSourceConfigBuilder negativeAckRedeliveryDelayMicros(long name) {
			config.set(PULSAR_NEGATIVE_ACK_REDELIVERY_DELAY_MICROS, name);
			return this;
		}
	 public static final ConfigOption<Integer>
     PULSAR_MAX_TOTAL_RECEIVER_QUEUE_SIZE_ACROSS_PARTITIONS =
             ConfigOptions.key(
                             CONSUMER_CONFIG_PREFIX
                                     + "maxTotalReceiverQueueSizeAcrossPartitions")
                     .intType()
                     .defaultValue(50000);
	 public PulsarSourceConfigBuilder maxTotalReceiverQueueSizeAcrossPartitions(int name) {
			config.set(PULSAR_MAX_TOTAL_RECEIVER_QUEUE_SIZE_ACROSS_PARTITIONS, name);
			return this;
		}
	 public static final ConfigOption<String> PULSAR_CONSUMER_NAME =
	            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "consumerName")
	                    .stringType()
	                    .noDefaultValue();
	 public PulsarSourceConfigBuilder consumerName(String name) {
			config.set(PULSAR_CONSUMER_NAME, name);
			return this;
		}
	 public static final ConfigOption<Long> PULSAR_ACK_TIMEOUT_MILLIS =
	            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "ackTimeoutMillis")
	                    .longType()
	                    .defaultValue(0L);
	 public PulsarSourceConfigBuilder ackTimeoutMillis(long name) {
			config.set(PULSAR_ACK_TIMEOUT_MILLIS, name);
			return this;
		}
	 public static final ConfigOption<Integer> PULSAR_MAX_REDELIVER_COUNT =
	            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "maxRedeliverCount")
	                    .intType()
	                    .noDefaultValue();
	 public PulsarSourceConfigBuilder maxRedeliverCount(int name) {
			config.set(PULSAR_MAX_REDELIVER_COUNT, name);
			return this;
		}
	 public static final ConfigOption<Boolean> PULSAR_POOL_MESSAGES =
	            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "poolMessages")
	                    .booleanType()
	                    .defaultValue(false);
	 public PulsarSourceConfigBuilder poolMessages(boolean name) {
			config.set(PULSAR_POOL_MESSAGES, name);
			return this;
		}

	public Configuration build() {
		// TODO Auto-generated method stub
		return this.config;
	}
	 
	
}
