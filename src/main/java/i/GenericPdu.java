package i;

import java.util.Map;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;

public class GenericPdu {

	public String name;
	public Map<String,Object> pdu;
	
	public GenericPdu() {
		
	}

	public GenericPdu(String name, Map<String, Object> pdu_load) {
		super();
		this.name = name;
		this.pdu = pdu_load;
	}
	
	@JsonIgnore
	public Integer getTimeOfPdu() {
		
		if( pdu.containsKey("ts"))
			return (Integer)pdu.get("ts");
		
		return Integer.valueOf(-1);
		
	}

}
