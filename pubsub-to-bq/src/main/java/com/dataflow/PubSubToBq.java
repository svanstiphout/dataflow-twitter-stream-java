package com.dataflow;

import java.io.IOException;
import java.util.List;
import java.util.Iterator;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.Date;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.api.services.bigquery.model.TableRow;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.util.Transport;

public class PubSubToBq {
  
	static class CleanDataFn extends DoFn<String, String> {
	/* Clean Tweet data */

	public Timestamp parseDateStringTimestamp(String dateString) {
	/* Convert date string to timestamp string */	
	    SimpleDateFormat format = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
        Timestamp fromTS = null;
        try {
          Date parsed = format.parse(dateString);
 		  fromTS = new Timestamp(parsed.getTime());
        } catch(ParseException pe) {
          ;
        }
        return fromTS;
	}

	public JSONArray flattenJsonArray(JSONArray arr){
		/* Flatten JSONArray */
		JSONArray flatten_arr = new JSONArray();
		for (int i = 0; i < arr.length(); i++) {
			if (arr.get(i) instanceof JSONArray) {
				JSONArray nested_arr = flattenJsonArray(arr.getJSONArray(i));
				for (int j = 0; j < nested_arr.length(); j++) {
					flatten_arr.put(nested_arr.get(j));
				}
			} else {
				flatten_arr.put(arr.get(i));
			}
		}
		return flatten_arr;
	}

	public JSONArray parseJsonArray(JSONArray arr){
		/* Parse JSONArray */
  		JSONArray parsed_arr = new JSONArray();
	  		// Loop through JSON array
	  		for (int i = 0; i < arr.length(); i++) {
		  		// Parse JSONObject in JSONArray
				if (arr.get(i) instanceof JSONObject) {
					JSONObject nested_obj = parseJsonObject(arr.getJSONObject(i));
					parsed_arr.put(nested_obj);
				} else {
					parsed_arr.put(arr.get(i));
				}
			}
  		return parsed_arr;
	}

	public JSONObject parseJsonObject(JSONObject obj) {
		/* Parse JSONObject */
		JSONObject parsed_obj = new JSONObject();
		// Iterate over JSON object
		for(Iterator iterator = obj.keySet().iterator(); iterator.hasNext();) {
		    String key = (String) iterator.next();

		    // Ignore if null
	  		if (obj.isNull(key)) {
		  		;
	  		// Temporarily, ignore some fields not supported by the current BQ schema
		    // TODO: update BigQuery schema
			} else if (key.equals("video_info") || key.equals("scopes") || key.equals("withheld_in_countries") ||  
				key.equals("source_user_id") || key.equals("source_user_id_str") || key.equals("extended_tweet") || 
				key.equals("quoted_status") || key.equals("attributes")) {
			  	//System.out.println("\n" + key);
			  	//System.out.println(obj.get(key));
			  	;
		  	// Convert date time to Timestamp	
		  	} else if (key.equals("created_at")) {
		  		Timestamp fromTS = parseDateStringTimestamp(obj.getString(key));
		  		if (fromTS != null) {
		  			parsed_obj.put(key, fromTS);
		  		}
	  		// Parse nested JSONObject
		  	} else if (obj.get(key) instanceof JSONObject) {
		  		JSONObject nested_obj = parseJsonObject(obj.getJSONObject(key));
		  		if (nested_obj.length() > 0) {
		  			parsed_obj.put(key, nested_obj);
		  		}
	  		// Parse nested JSONArray
		  	} else if (obj.get(key) instanceof JSONArray) {
				// Flatten nested Arrays where needed
				JSONArray nested_arr = new JSONArray();
				if (key.equals("coordinates")) {
			  		nested_arr = flattenJsonArray(obj.getJSONArray(key));
			  	} else {
			  		nested_arr = parseJsonArray(obj.getJSONArray(key));
			  	}
		  		if (nested_arr.length() > 0) {
		  			parsed_obj.put(key, nested_arr);
		  		}
	  		// Add value to new object as-is
		  	} else {
		  		parsed_obj.put(key, obj.get(key));
		  	}
		}
		// Return parsed object
		return parsed_obj;
	}

		@ProcessElement
		public void processElement(ProcessContext c) {
		   JSONObject obj = new JSONObject(c.element());
		   JSONObject parsed_obj = parseJsonObject(obj);
		   c.output(parsed_obj.toString());
		}
	}

  static class ParseTableRowJson extends SimpleFunction<String, TableRow> {
    @Override
    public TableRow apply(String input) {
       try {
        return Transport.getJsonFactory().fromString(input, TableRow.class);
      } catch (IOException e) {
        throw new RuntimeException("Failed parsing table row json", e);
      }
    }
  }

  public interface Options extends PipelineOptions {
    @Description("Pub/Sub subscription to read from, specified as "
        + "projects/<project_id>/subscription/<subscription_name>")
    @Validation.Required
    String getInput();
    void setInput(String value);

    @Description("BigQuery table to write to, specified as "
        + "<project_id>:<dataset_id>.<table_id>. The dataset must already exist.")
    @Validation.Required
    String getOutput();
    void setOutput(String value);
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline p = Pipeline.create(options);

    p.apply(PubsubIO.readStrings().fromSubscription(options.getInput()))
     .apply(ParDo.of(new CleanDataFn()))
     .apply(MapElements.via(new ParseTableRowJson()))
     .apply(BigQueryIO.writeTableRows()
         .to(options.getOutput())
         .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
         .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

	p.run();
    }
}