package stormTP.operator;

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


import java.util.List;


/**
 * Sample of stateless operator
 * @author lumineau
 *
 */
public class MyTortoiseBolt implements IRichBolt {

	private static final long serialVersionUID = 4262369370788107343L;

	private static Logger logger = Logger.getLogger("NothingBoltLogger");
	private OutputCollector collector;
	
	
	public MyTortoiseBolt () {
		
	}
	
	/* (non-Javadoc)
	 * @see backtype.storm.topology.IRichBolt#execute(backtype.storm.tuple.Tuple)
	 */
	public void execute(Tuple t) {
	
		try {
			String n = t.getValueByField("json").toString();

			/* Exraction des valeurs pour les encrypter */
			JsonReader jr = Json.createReader( new StringReader(n) );
			JsonObject obj = jr.readObject();

			JsonArray l = obj.getJsonArray("runners");

			for (JsonValue v : l){
					JsonObject turtle = v.asJsonObject();
					if (turtle.getInt("id")==3){

						collector.emit(t, new Values(
							turtle.getInt("id"),
							turtle.getInt("top"),
							"Michelangelo",
							(turtle.getInt("tour")*turtle.getInt("maxcel"))+turtle.getInt("cellule"),
							turtle.getInt("total"),
							turtle.getInt("maxcel")
						));
						logger.info( "=> " + "Turtle 3 found" + " treated!");
						collector.ack(t);
				}
				logger.info( "=> " + "Not turtle 3 found" + " treated!");
			}

		}catch (Exception e){
			System.err.println("Empty tuple.");
			System.err.println(e);
		}
		return;
		
	}
	
	
	
	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declare(new Fields("id","top","nom","nbCellsParcourus","total","maxcel"));
	}
		

	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#getComponentConfiguration()
	 */
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	/* (non-Javadoc)
	 * @see backtype.storm.topology.IBasicBolt#cleanup()
	 */
	public void cleanup() {
		
	}
	
	
	
	/* (non-Javadoc)
	 * @see backtype.storm.topology.IRichBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */
	@SuppressWarnings("rawtypes")
	public void prepare(Map arg0, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
}