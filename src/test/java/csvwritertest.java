import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;

//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.dataformat.csv.CsvMapper;
//import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import m.Boot;

public class csvwritertest {

	public csvwritertest() {
		
	}
	
	public static void main(String[] args) throws JsonProcessingException {
		CsvMapper csvMapper = new CsvMapper();
		CsvSchema  schema = csvMapper.schemaFor(Boot.PayloadRecord.class)
				.withQuoteChar('"')
				.withColumnSeparator('|');
		Object o = new Boot.PayloadRecord("23434","{}");
		System.out.println(csvMapper.writerFor(Boot.PayloadRecord.class).with(schema)
				.writeValueAsString(o));
		
	}

}
