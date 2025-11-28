package stormTP.operator;


import java.util.Map;
//import java.util.logging.Logger;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import stormTP.stream.StreamEmiter;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;


public class ExitBolt4 implements IRichBolt {

	private static final long serialVersionUID = 4262369370788107342L;
	//private static Logger logger = Logger.getLogger("ExitBolt");
	private OutputCollector collector;
	int port = -1;
	StreamEmiter semit = null;
	
	public ExitBolt4 (int port) {
		this.port = port;
		this.semit = new StreamEmiter(this.port);
		
	}
	
	/* (non-Javadoc)
	 * @see backtype.storm.topology.IRichBolt#execute(backtype.storm.tuple.Tuple)
	 */
	public void execute(Tuple t) {
	
		int id =(int) t.getValueByField("id");
		int top =(int) t.getValueByField("top");
		String rank = t.getValueByField("rang").toString();
		int total =(int) t.getValueByField("total");
		int maxcel =(int) t.getValueByField("maxcel");


		JsonObjectBuilder r = null;
		r = Json.createObjectBuilder();

		r.add("id", id);
		r.add("top", top);
		r.add("rang", rank);
		r.add("total", total);
		r.add("maxcel", maxcel);
		
		JsonObject row = r.build();

		this.semit.send(row.toString());
		collector.ack(t);
		
		return;
		
	}
	

	
	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declare(new Fields("json"));
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