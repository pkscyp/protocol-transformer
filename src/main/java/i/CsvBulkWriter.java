package i;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonEncoding;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;




public class CsvBulkWriter<IN> implements BulkWriter<IN>{

	private static final Charset CHARSET = StandardCharsets.UTF_8;

    private final FSDataOutputStream stream;
    final CsvMapper csvMapper;
    final CsvSchema schema ;
    final Class<IN> _class;
    private final JsonGenerator generator;

	public CsvBulkWriter(FSDataOutputStream stream, Class<IN> _class, CsvMapper csvMapper, CsvSchema schema) {
		this.stream = Preconditions.checkNotNull(stream);
		this._class=_class;
		this.schema=schema;
		this.csvMapper=csvMapper;
		try {
			this.csvMapper.getFactory().disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
			this.csvMapper.disable(SerializationFeature.FLUSH_AFTER_WRITE_VALUE);
			this.generator =  this.csvMapper.writer(schema).createGenerator(stream, JsonEncoding.UTF8);
		} catch (IOException e) {
			throw new FlinkRuntimeException("Could not create CSV generator.", e);
		}
	}



	@Override
	public void addElement(IN element) throws IOException {
	   // String wr=	csvMapper.writerFor(this._class).with(schema).writeValueAsString(element);
		//this.stream.write(wr.getBytes(CHARSET));
		generator.writeObject(element);
	}

	@Override
	public void finish() throws IOException {
		generator.close();
        stream.sync();
		
	}

	@Override
	public void flush() throws IOException {
		generator.flush();
		
	}
	
	public static final class CsvBulkWriterFactory<IN>
    implements BulkWriter.Factory<IN> {

		private static final long serialVersionUID = 1L;
		
		final CsvSchema schema;
		final CsvMapper csvMapper;
		final Class<IN> _class;
		

		public CsvBulkWriterFactory(Class<IN> a) {
			super();
			this._class = a;
			this.csvMapper = new CsvMapper();
			this.schema = csvMapper.schemaFor(a).withoutQuoteChar().withColumnSeparator('|');
			
		}

		@Override
		public BulkWriter<IN> create(FSDataOutputStream out) {
		    return new CsvBulkWriter<IN>(out,this._class,this.csvMapper,this.schema);
		}
		
		
	}

}
