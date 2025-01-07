package i;

import java.io.Serializable;
import java.util.UUID;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;

public abstract class AbstractAdu implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public  String adu_id;
	public final String bundle_id;
	public final Integer ts;
	private final String class_name;
	public Double latitude;
	public Double longitude;
	
	public AbstractAdu(String bundle_id,Integer its,String class_name) {
		this.ts=its;
		this.class_name = class_name;
		this.bundle_id = bundle_id;
		this.adu_id = UUID.randomUUID().toString();
	}
	
	@JsonIgnore
	public String getClassName() { return this.class_name; }
	
	
}
