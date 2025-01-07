package u;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.text.StringSubstitutor;
import org.apache.commons.text.lookup.StringLookupFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.YamlParserUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.snakeyaml.engine.v2.api.Load;
import org.snakeyaml.engine.v2.api.LoadSettings;
import org.snakeyaml.engine.v2.schema.CoreSchema;

public final class FlinkSettings {

	private static final Logger logger = LoggerFactory.getLogger(FlinkSettings.class);
	
	public FlinkSettings() {
		// TODO Auto-generated constructor stub
	}
	
	static public  Configuration createConfiguration() throws IOException {
		StringSubstitutor  stringSubstitutor=
				new StringSubstitutor(StringLookupFactory.INSTANCE.environmentVariableStringLookup());
		File config = new File("conf/config.yaml");
		 String contents =
		          stringSubstitutor.replace(new String(Files.readAllBytes(config.toPath())));
		 logger.debug(contents);
		 
		 Load loader = new Load(LoadSettings.builder().setSchema(new CoreSchema()).build());
		 Map<String, Object> yamlResult =
				 (Map<String, Object>)loader.loadAllFromString(contents).iterator().next();
		 
		 Map<String, Object> configMap =  flatten(yamlResult,"");
		 Map<String,String> newMap = configMap.entrySet().stream()
			     .collect(Collectors.toMap(Map.Entry::getKey, e -> String.valueOf(e.getValue())));

		 Configuration conf = Configuration.fromMap(newMap);
		 return conf;
	}

	private static Map<String, Object> flatten(Map<String, Object> config, String keyPrefix) {
        final Map<String, Object> flattenedMap = new HashMap<>();

        config.forEach(
                (key, value) -> {
                    String flattenedKey = keyPrefix + key;
                    if (value instanceof Map) {
                        Map<String, Object> e = (Map<String, Object>) value;
                        flattenedMap.putAll(flatten(e, flattenedKey + "."));
                    } else {
                        if (value instanceof List) {
                            flattenedMap.put(flattenedKey, YamlParserUtils.toYAMLString(value));
                        } else {
                            flattenedMap.put(flattenedKey, value);
                        }
                    }
                });

        return flattenedMap;
    }
}
