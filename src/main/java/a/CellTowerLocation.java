package a;

import java.util.List;
import java.util.Map;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;

import i.AbstractAdu;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class CellTowerLocation extends AbstractAdu {

	
	private static final long serialVersionUID = 1L;
	public String source="ctp";
	public List<Map<String,Object>> cell_towers;
	
	public CellTowerLocation(String bundle_id,Integer its,String class_name) {
		super(bundle_id,its,class_name);
		
	}

	@Override
	public String toString() {
		
		return "";
	}

}
