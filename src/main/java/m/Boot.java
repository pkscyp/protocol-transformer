package m;


import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.jackson.JacksonMapperFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import f.AckController;
import f.BuildPduContext;
import f.ConvertToAdus;
import i.AduContext;
import i.CsvBulkWriter;
import i.GenericPdu;
import i.PduContext;
import u.FlinkSettings;
import u.JSTransformerHelper;
import u.JsonUtils;

public class Boot {

	private static final Logger logger = LoggerFactory.getLogger(Boot.class);
	
	public Boot() {
		
	}

	public static void main(String[] args) throws IOException {

		Configuration conf = FlinkSettings.createConfiguration();
		
		JSTransformerHelper.with();
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
		

		env.setParallelism(3);
		env.setMaxParallelism(4);
		env.enableChangelogStateBackend(false);
		File file = new File("data/5000_pdu_bundle.csv");
		File outfolder =  new File("data/tx");
		 
		FileSink<AduPayloadRecord> sink =
                FileSink.forBulkFormat(
                                new Path(outfolder.toURI()),
                                new CsvBulkWriter.CsvBulkWriterFactory<AduPayloadRecord>(AduPayloadRecord.class)
                                )
                 		.withOutputFileConfig(new OutputFileConfig("adus-", ".csv"))
                        .build();
		
		FileSink<AtomicPdu> sinkPdu =
                FileSink.forBulkFormat(
                                new Path(outfolder.toURI()),
                                new CsvBulkWriter.CsvBulkWriterFactory<AtomicPdu>(AtomicPdu.class)
                                )
                		.withOutputFileConfig(new OutputFileConfig("pdus-", ".csv"))
                        .build();
		
		CsvReaderFormat<PayloadRecord> format =  CsvReaderFormat.forSchema(
        		JacksonMapperFactory::createCsvMapper, 
        		mapper -> mapper.schemaFor(PayloadRecord.class).withQuoteChar('\"').withColumnSeparator(',').withSkipFirstDataRow(true), 
        		TypeInformation.of(PayloadRecord.class));
		
		
		FileSource<PayloadRecord> source = FileSource.forRecordStreamFormat(format, Path.fromLocalFile(file)).build();
		
		try {
			final OutputTag<PduContext> outputTag = new OutputTag<PduContext>("side-output-pdus"){
				private static final long serialVersionUID = 1L;
				};
			
			final DataStream<PayloadRecord> stream =  env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");
			
			final SingleOutputStreamOperator<PduContext> process_stream=	stream
			.keyBy((r) -> r.imei)
			.map(new BuildPduContext())
			.process(new ProcessFunction<PduContext,PduContext>(){
				private static final long serialVersionUID = 1L;
				@Override
				public void processElement(PduContext element, ProcessFunction<PduContext, PduContext>.Context ctx,
						Collector<PduContext> out) throws Exception {
					out.collect(element);					
					ctx.output(outputTag,element);					
				}				
			});
			process_stream.getSideOutput(outputTag)
			.flatMap((PduContext r,Collector<AtomicPdu> out) -> {
				 for(GenericPdu p: r.getPdus()) {
					 out.collect(new AtomicPdu(r.imei,p.name,JsonUtils.with().stringify(p.pdu)));
				 }
				}		
			)
			.returns(AtomicPdu.class)
			.sinkTo(sinkPdu).setParallelism(1);
			
			process_stream.map(new ConvertToAdus())
			.flatMap((AduContext r, Collector<AduPayloadRecord> out) ->{
				Integer maxAdus = r.ads.size();
				
				for(int i=0;i<r.ads.size();i++) {
					AduPayloadRecord pr = new AduPayloadRecord();
					pr.imei = r.imei;
					pr.bid = r.bid;
					pr.maxAduCount = maxAdus;
					pr.payload = r.mapper.writeValueAsString(r.ads.get(i));
					out.collect(pr);
				}
				if(maxAdus <= 0) {
					AduPayloadRecord pr = new AduPayloadRecord();
					pr.imei = r.imei;
					pr.bid = r.bid;
					pr.maxAduCount = 0;
					out.collect(pr);
				}
			})
			.returns(AduPayloadRecord.class)
			.map(new AckController())
			.filter(x -> x.maxAduCount>0)
			.sinkTo(sink).setParallelism(1)
			
			;	
			

			env.execute("Dump All CSV text");
		} catch (Exception e) {
			logger.error("ERROR", e);
		}

	}
	
	@JsonPropertyOrder({"imei","pduname","pudinfo"})
	public static class AtomicPdus {
		private static ObjectMapper mapper = new ObjectMapper();
		public String imei;
		public String pdu_name;
		public Map<String,Object> pdu_info;
		public AtomicPdus(String imei, String pduname, Map<String, Object> pudinfo) {
			super();
			this.imei = imei;
			this.pdu_name = pduname;
			this.pdu_info = pudinfo;
		}
		public String toString() {
			try {
				return mapper.writeValueAsString(this);
			} catch (JsonProcessingException e) {
				
			}
			return null;
		}
	}
	@JsonPropertyOrder({"imei","payload"})
	public static class AduPayloadRecord {
		
		public String imei;
		public String payload;
		@JsonIgnore
		public String bid;
		@JsonIgnore
		public Integer maxAduCount;
		
		public AduPayloadRecord() {
			
		}

		public AduPayloadRecord(String imei, String payload) {
			super();
			this.imei = imei;
			this.payload = payload;
		}
		
		
		

		public String toString() {
			return imei+":"+payload;
		}
		
	}
	
	@JsonPropertyOrder({"imei","payload"})
	public static class PayloadRecord {
		
		public String imei;
		public String payload;
		
		public PayloadRecord() {
			
		}

		public PayloadRecord(String imei, String payload) {
			super();
			this.imei = imei;
			this.payload = payload;
		}
		
		
		

		public String toString() {
			return imei+":"+payload;
		}
		
	}
	
	public static final class KeyBucketAssigner   implements BucketAssigner<PayloadRecord, String> {

		private static final long serialVersionUID = 987325769970523326L;

		@Override
		public String getBucketId(final PayloadRecord element, final Context context) {
		    return element.imei;
		}
		
		@Override
		public SimpleVersionedSerializer<String> getSerializer() {
		    return SimpleVersionedStringSerializer.INSTANCE;
		}
	}
	
	@JsonPropertyOrder({"imei","pdu_name","payload"})
	public static final class AtomicPdu {
		
		
		public String imei;
		public String pdu_name;
		public String payload;
		public AtomicPdu(String imei, String pdu_name, String payload) {
			super();
			this.imei = imei;
			this.pdu_name = pdu_name;
			this.payload = payload;
		}
		
		
		
	}

}
