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
import java.util.ArrayList; 


import java.util.List;


/**
 * Sample of stateless operator
 * @author lumineau
 *
 */
public class GiveRankBolt implements IRichBolt {

	private static final long serialVersionUID = 4262369370788107343L;

	private static Logger logger = Logger.getLogger("NothingBoltLogger");
	private OutputCollector collector;
	
	
	public GiveRankBolt () {
		
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

			List<JsonObject> tortueList = new ArrayList();
			List<Integer> places = new ArrayList();

			for (JsonValue v : l){
					JsonObject turtle = v.asJsonObject();
					tortueList.add(turtle);
					places.add((turtle.getInt("tour")*turtle.getInt("maxcel"))+turtle.getInt("cellule"));
			}	

			logger.info("Debut assignation Rang ==>");
			for (int i=0; i < tortueList.size(); i++){
				int current_score = places.get(i);
				int nb_before = 0;
				int nb_ex = 0;
				for (int j : places){
					if (j>current_score){
						nb_before++;
					}
					if (j==current_score){
						nb_ex++;
					}
				}
				JsonObject turtle = tortueList.get(i);

				if (nb_ex > 1 ){
					collector.emit(t, new Values(
							turtle.getInt("id"),
							turtle.getInt("top"),
							String.valueOf(nb_before+1)+"-ex",
							turtle.getInt("total"),
							turtle.getInt("maxcel")
						));

						logger.info( "=> treated! valules: id="
						+String.valueOf(turtle.getInt("id"))+", top="+
							String.valueOf(turtle.getInt("top"))+", rang="+
							String.valueOf(nb_before+1)+"-ex"+", total="+
							String.valueOf(turtle.getInt("total"))+", maxcel="+
							String.valueOf(turtle.getInt("maxcel"))
						);
				}else{
					collector.emit(t, new Values(
							turtle.getInt("id"),
							turtle.getInt("top"),
							String.valueOf(nb_before+1),
							turtle.getInt("total"),
							turtle.getInt("maxcel")
						));

					logger.info( "=> treated! valules: id="
						+String.valueOf(turtle.getInt("id"))+", top="+
							String.valueOf(turtle.getInt("top"))+", rang="+
							String.valueOf(nb_before+1)+", total="+
							String.valueOf(turtle.getInt("total"))+", maxcel="+
							String.valueOf(turtle.getInt("maxcel"))
						);	
				}
				
			}

			logger.info("<== FIn assignation Rang");
			collector.ack(t);

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
		arg0.declare(new Fields("id","top","rang","total","maxcel"));
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