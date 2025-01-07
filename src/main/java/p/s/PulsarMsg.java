package p.s;

import java.io.Serializable;

public class PulsarMsg implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public  Integer readerId;
	public  String msg;
	public String imei;
	public String payload;
	
	
	public PulsarMsg() {
		super();
		// TODO Auto-generated constructor stub
	}

	public PulsarMsg(Integer readerId,String msg, String imei) {
		this.readerId = readerId;
		this.msg = msg;
		this.imei=imei;
	}

	

	@Override
	public String toString() {
		return "PulsarMsg [readerId=" + readerId + ", msg=" + msg + ", imei=" + imei + "]";
	}

	public Integer getReaderId() {
		return readerId;
	}

	public PulsarMsg setReaderId(Integer readerId) {
		this.readerId = readerId;
		return this;
	}

	public String getMsg() {
		return msg;
	}

	public PulsarMsg setMsg(String msg) {
		this.msg = msg;
		return this;
	}

	public String getImei() {
		return imei;
	}

	public PulsarMsg setImei(String imei) {
		this.imei = imei;
		return this;
	}

	public String getPayload() {
		return payload;
	}

	public PulsarMsg setPayload(String payload) {
		this.payload = payload;
		return this;
	}

	
	
}
