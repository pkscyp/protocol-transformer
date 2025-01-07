package u;


import java.util.List;
import java.util.concurrent.Callable;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtils  {

	final  ObjectMapper mapper;
	
	private static final OnDemandInit<JsonUtils> DEFAULT = new OnDemandInit<JsonUtils>(new Callable<JsonUtils>() {

		@Override
		public JsonUtils call() throws Exception {
			JsonUtils inst = new JsonUtils();
			return inst;
		}		
	});
	public static JsonUtils with() {
		return DEFAULT.get();
	}
	
	
	private JsonUtils()  {
		this.mapper=new ObjectMapper();
	}

	
	public  String stringify(Object o) throws Exception {
		return mapper.writeValueAsString(o);
	}
	
	public Object parse(String s,Class<Object> type) throws Exception {
		return mapper.readValue(s, type);
	}
	
	
	
}
