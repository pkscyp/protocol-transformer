import java.util.Map;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class testring {

	public testring() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) throws JsonProcessingException {
		System.out.println("Ali\\'s S23 FE");
		ObjectMapper mapper = new ObjectMapper();
		Map<String,Object> v = Map.of("Name","Ali\\'s S23 FE");
		String json = mapper.writeValueAsString(v);
		System.out.println(json);
		Map<String,Object> v2 = mapper.readValue(json,Map.class );

	}

}
