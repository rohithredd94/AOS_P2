import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;


@SuppressWarnings("serial")
public class Message implements Serializable {
	NodeServer m = new NodeServer();
	int n = m.num_of_nodes;
}
@SuppressWarnings("serial")
// Application Message consists of just a string and the vector timestamp
class ApplicationMsg extends Message implements Serializable{
	String msg = "hello";
	int node_id;
	int[] current_time_stamp;
}
// Marker Msg is used to just send it to its neighbors so just a string
@SuppressWarnings("serial")
class MarkerMsg extends Message implements Serializable{
	String msg = "marker";
	int node_id;
}
// State message is sent over converge cast tree , state message should have
// the process state and all its incoming channel states 
@SuppressWarnings("serial")
class StateMsg extends Message implements Serializable{
	boolean active;
	int node_id;
	HashMap<Integer,ArrayList<ApplicationMsg>> in_transit_msgs;
	int[] current_time_stamp;
}
// Finish message is sent by Node 0 to all the other nodes in the system
// to bring the entire system to halt and write to output console
@SuppressWarnings("serial")
class FinishMsg extends Message implements Serializable{
	String msg = "halt";
}