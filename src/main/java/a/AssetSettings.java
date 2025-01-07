package a;

import java.util.Map;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;

import i.AbstractAdu;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class AssetSettings extends AbstractAdu {

	private static final long serialVersionUID = 1L;
	public String source;
	public String name;
	public Map<String,Object> addinfo;
	
	public AssetSettings(String bundle_id,Integer its,String class_name) {
		super(bundle_id,its,class_name);
		
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "";
	}

}
