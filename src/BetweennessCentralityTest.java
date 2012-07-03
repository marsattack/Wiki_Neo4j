import java.util.HashSet;
import java.util.Set;

import org.graphipedia.dataimport.ExtractLinks;
import org.graphipedia.dataimport.neo4j.ImportGraph;
import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.impl.centrality.BetweennessCentrality;
import org.neo4j.graphalgo.impl.shortestpath.SingleSourceShortestPath;
import org.neo4j.graphalgo.impl.shortestpath.SingleSourceShortestPathDijkstra;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

public class BetweennessCentralityTest 
{
	private static GraphDatabaseService graphDb;
	
	public static void main(String[] args)  {
    	String originalFile = "J:/enwiki-latest-pages-articles.xml.bz2";
    	String inputFile = "J:/Wiki-GraphDB.xml";
        String dataDir = "J:/Wiki-GraphDB/";
 
		try {
	    	ExtractLinks self = new ExtractLinks();
	    	self.extract(originalFile, inputFile);
	    	
            ImportGraph importer = new ImportGraph(dataDir);
            importer.createNodes(inputFile);
            importer.createRelationships(inputFile);

	    } catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( dataDir );
        registerShutdownHook();
        
        Set<Node> allNodes = new HashSet<Node>();
        for (Node node:  GlobalGraphOperations.at(graphDb).getAllNodes()){
        	allNodes.add(node);
        	System.out.println(node.getId());
        }
        System.out.println("dataset initialized");
        
        BetweennessCentrality<Double> betweennessCentrality = new BetweennessCentrality<Double>(
            getSingleSourceShortestPath(), allNodes);
        betweennessCentrality.calculate();
            
        for (Node node:allNodes){
        	System.out.print("betweenness centrality of node: " + node.getId() + " is ");
        	Double result = betweennessCentrality.getCentrality(node);
        	System.out.println(result);
        }
        shutdown();
	}
	
	protected static SingleSourceShortestPath<Double> getSingleSourceShortestPath()
    {
        return new SingleSourceShortestPathDijkstra<Double>( 0.0, null,
            new CostEvaluator<Double>()
            {
                public Double getCost( Relationship relationship,
                            Direction direction )
                {
                    return 1.0;
                }
            }, new org.neo4j.graphalgo.impl.util.DoubleAdder(),
            new org.neo4j.graphalgo.impl.util.DoubleComparator(),
            Direction.BOTH, RelTypes.LINKS );
    }
	private static void registerShutdownHook()
    {
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            public void run()
            {
                shutdown();
            }
        } );
    }
    private static void shutdown()
    {
        graphDb.shutdown();
    }
	public enum RelTypes implements RelationshipType
	{
	    LINKS
	}
}
