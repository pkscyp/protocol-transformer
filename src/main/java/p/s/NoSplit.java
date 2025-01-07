package p.s;

import java.io.Serializable;

import org.apache.flink.api.connector.source.SourceSplit;

public class NoSplit implements SourceSplit, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private Integer id;
	
	private Boolean nowStop=false;
	
	public NoSplit() {this.id=-1;}

	public NoSplit(Integer id) {
		this(id,false);
	}
	public NoSplit(Integer id,Boolean toStop) {
		this.id=id;
		this.nowStop=toStop;
	}
	
	@Override
	public String splitId() {
		// TODO Auto-generated method stub
		return String.valueOf(id);
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public Boolean getNowStop() {
		return nowStop;
	}

	public void setNowStop(Boolean nowStop) {
		this.nowStop = nowStop;
	}

	@Override
	public String toString() {
		return "NoSplitKeyShared [id=" + id + ", nowStop=" + nowStop + "]";
	}

	
	
}
