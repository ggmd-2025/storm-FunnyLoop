package stormTP.operator;


import java.util.Map;


import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseStatefulBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


import java.util.Map;
import java.util.logging.Logger;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import javax.json.JsonArray;
import javax.json.JsonValue;
import java.io.StringReader;
import java.util.ArrayList; 

import java.util.List;


public class ComputeBonusBolt extends BaseStatefulBolt<KeyValueState<String, Integer>> {
	private static final long serialVersionUID = 4262379330722107343L;
    KeyValueState<String, Integer> kvState;
    int sum;
    private OutputCollector collector;

    
    @Override
    public void execute(Tuple t) {

		/** "id","top","rang","total","maxcel" */

		int id = (int) t.getValueByField("id");



			/* Exraction des valeurs pour les encrypter */
		JsonReader jr = Json.createReader( new StringReader(n) );
		JsonObject obj = jr.readObject();

			JsonArray l = obj.getJsonArray("runners");

			List<JsonObject> tortueList = new ArrayList();
			List<Integer> places = new ArrayList();

			for (JsonValue v : l){
				JsonObject turtle = v.asJsonObject();
				tortueList.add(turtle);
			}	
    	
    	sum++;
		
		kvState.put("sum", sum);
		    
	    JsonObjectBuilder r = Json.createObjectBuilder();
	    r.add("test", "statelessWithWindow");
	    r.add("nbNewTuples", 1);
		r.add("totalNumberOfTuples", sum);
	    JsonObject row = r.build();
		    
	        collector.emit(t,new Values(row.toString()));
    }

    @Override
    public void initState(KeyValueState<String, Integer> state) {
        kvState = state;
        sum = kvState.get("sum", 0);
        
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id","tops","nom","poin"));
    }


}