package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import stormTP.operator.ExitBolt4;
import stormTP.operator.InputStreamSpout;
import stormTP.operator.NothingBolt;
import stormTP.operator.ComputeBonusBolt;
import stormTP.operator.GiveRankBolt;


/**
 * 
 * @author lumineau
 * Topologie test permettant d'écouter le Master Input 
 *
 */
public class TopologyT4 {
	
	public static void main(String[] args) throws Exception {
		int nbExecutors = 1;
		int portINPUT = Integer.parseInt(args[0]);
		int portOUTPUT = Integer.parseInt(args[1]);
    	
		/*Création du spout*/
    	InputStreamSpout spout = new InputStreamSpout("127.0.0.1", portINPUT);
    	/*Création de la topologie*/
    	TopologyBuilder builder = new TopologyBuilder();
        /*Affectation à la topologie du spout*/
        builder.setSpout("masterStream", spout);
        /*Tortoise*/
        builder.setBolt("rang", new ComputeBonusBolt(), nbExecutors).shuffleGrouping("masterStream");

        builder.setBolt("bonus", new GiveRankBolt(), nbExecutors).shuffleGrouping("rang");
        /*Affectation à la topologie du bolt qui émet le flux de sortie, il prendra en input le bolt nofilter*/
        builder.setBolt("exit", new ExitBolt4(portOUTPUT), nbExecutors).shuffleGrouping("bonus");
       
        /*Création d'une configuration*/
        Config config = new Config();
        /*La topologie est soumise à STORM*/
        StormSubmitter.submitTopology("topoT2", config, builder.createTopology());

	}
		
	
}