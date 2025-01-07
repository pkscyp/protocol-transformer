package a;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import i.AbstractAdu;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class GpsLocation extends AbstractAdu {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static ObjectMapper mapper = new ObjectMapper();
	public String source="sa";
	public Integer altitude;
	public Integer speed;
	public Integer error;
	public Integer heading;
	public HashMap<String,Object> addinfo;
	
	public GpsLocation(String bundle_id,Integer its,String class_name) {
		super(bundle_id,its,class_name);
		
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		try {
			return mapper.writeValueAsString(this);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "ERRORJSON";
	}

	public HashMap<String, Object> getAddinfo() {
		return addinfo;
	}

	

}
