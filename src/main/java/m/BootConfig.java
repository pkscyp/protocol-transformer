package m;

import java.io.IOException;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import u.FlinkSettings;


public class BootConfig {

	private static final Logger logger = LoggerFactory.getLogger(BootConfig.class);
	
	private static final String KEY_SEPARATOR = ".";
	
	public BootConfig() {
		
	}

	public static void main(String[] args) throws IOException {
		
		 Configuration conf = FlinkSettings.createConfiguration();
		 final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
		 logger.info("{}",env.getConfiguration());

		 
		 
		 

	}
	

}
