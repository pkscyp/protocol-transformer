package a;

import java.util.Map;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;

import i.AbstractAdu;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class AssetState extends AbstractAdu {

	private static final long serialVersionUID = 1L;
	public String source;
	public Integer battery_level;
	public Integer on_charger;
	public Integer under_temper;
	public Integer wifi_count;
	public Integer cell_tower_count;
	public Integer paired_asset_count;
	public Integer motion;
	public Integer tz_offset;
	public Map<String,Object> addinfo;
	
	public AssetState(String bundle_id,Integer its,String class_name) {
		super(bundle_id,its,class_name);
		
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "";
	}

}
