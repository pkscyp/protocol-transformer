package a;

import java.util.Map;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;

import i.AbstractAdu;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Connectivity extends AbstractAdu {

	private static final long serialVersionUID = 1L;
	public String source;
	public String type;
	public String connected_to;
	public Map<String,Object> addinfo;
	
	public Connectivity(String bundle_id,Integer its,String class_name) {
		super(bundle_id,its,class_name);
		
	}

	

}
