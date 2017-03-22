import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.Set;

@SuppressWarnings("serial")
public class ProjectMain implements Serializable  {
	static String outputFileName;
	int id;
	int num_of_nodes, min_per_active, max_per_active, min_send_delay, snapshot_delay, max_number;
	int total_messages_sent = 0;
	boolean active=false;
	int[][] adj_matrix;
	int[] current_time_stamp;
	int[] neighbors;
	boolean is_blue = true;
	int logging = 0;
	boolean is_first_time = true;
	//ArrayLintst which holds the nodes part of the distributed system 
	ArrayList<Node> nodes = new ArrayList<Node>();
	//HashMap which has node number as keys and <id,host,port> as value
	// Create all the channels in the beginning and keep it open till the end
	HashMap<Integer,Socket> channels = new HashMap<Integer,Socket>();
	// Create all the output streams associated with each socket 
	HashMap<Integer,ObjectOutputStream> output_stream = new HashMap<Integer,ObjectOutputStream>();
	// HashMap which stores ArrayList of messages recorded while the process is red for each channel
	HashMap<Integer,ArrayList<ApplicationMsg>> in_transit_msgs;
	// HashMap which stores all incoming channels and boolean received marker message
	HashMap<Integer,Boolean> rec_marker;
	// HashMap which stores all state messages
	HashMap<Integer,StateMsg> state_messages;	
	//Used to determine if state message has been received from all the processes in the system
	boolean[] nodesInGraph;
	//Every process stores its state(current_time_stamp,in_transit_msgs and its id) in this StateMsg Object
	StateMsg myState;
	//To hold output Snapshots
	ArrayList<int[]> output = new ArrayList<int[]>();

	//Re-initialize everything that is needed for Chandy Lamport protocol before restarting it
	void initialize(ProjectMain mainObj){
		mainObj.in_transit_msgs = new HashMap<Integer,ArrayList<ApplicationMsg>>();
		mainObj.rec_marker = new HashMap<Integer,Boolean>();
		mainObj.state_messages = new HashMap<Integer,StateMsg>();	

		Set<Integer> keys = mainObj.channels.keySet();
		//Initialize in_transit_msgs hashMap
		for(Integer element : keys){
			ArrayList<ApplicationMsg> arrList = new ArrayList<ApplicationMsg>();
			mainObj.in_transit_msgs.put(element, arrList);
		}
		//Initialize boolean hashmap rec_marker to false
		for(Integer e: mainObj.neighbors){
			mainObj.rec_marker.put(e,false);
		}
		mainObj.nodesInGraph = new boolean[mainObj.num_of_nodes];
		mainObj.myState = new StateMsg();
		mainObj.myState.current_time_stamp = new int[mainObj.num_of_nodes];
	}


	public static void main(String[] args) throws IOException, InterruptedException {
		//Read the values for all variables from the configuration file
		ProjectMain mainObj = ConfigParser.readConfigFile(args[1]);
		// Get the node number of the current Node
		mainObj.id = Integer.parseInt(args[0]);
		int curNode = mainObj.id;
		//Get the configuration file from command line
		ProjectMain.outputFileName = mainObj.args[1].substring(0, mainObj.args[1].lastIndexOf('.'));
		//Build converge cast spanning tree in the beginning
		ConvergeCast.buildSpanningTree(mainObj.adj_matrix);
		// Transfer the collection of nodes from ArrayList to hash map which has node id as key since  
		// we need to get and node as value ,it returns <id,host,port> when queried with node Id.
		HashMap<Integer, Node> node_address_list = new HashMap<Integer, Node>();
		for(int i=0;i<mainObj.nodes.size();i++){
			node_address_list.put(mainObj.nodes.get(i).nodeId, mainObj.nodes.get(i));
		}
		// Get the port number on which this node should listen 
		int serverPort = mainObj.nodes.get(mainObj.id).port;
		// Start server on this node's assigned port
		ServerSocket listener = new ServerSocket(serverPort);
		Thread.sleep(10000);
		//Create channels and keep it till the end
		for(int i=0;i<mainObj.num_of_nodes;i++){
			// If the value in adjacency matrix is one for the current Node then its a neighbor
			if(mainObj.adj_matrix[curNode][i] == 1){
				InetAddress address = InetAddress.getByName(hostName);
				Socket client = new Socket(node_address_list.get(i).host, node_address_list.get(i).port);
				mainObj.channels.put(i, client);
				// Get an output stream associated with each socket and put it in a hashmap output_stream
				ObjectOutputStream oos = new ObjectOutputStream(client.getOutputStream());
				mainObj.output_stream.put(i, oos);
			}
		}

		//Populate neighbors array 
		Set<Integer> keys = mainObj.channels.keySet();
		mainObj.neighbors = new int[keys.size()];
		int index = 0;
		for(Integer element : keys) mainObj.neighbors[index++] = element.intValue();
		//mainObj.current_time_stamp is used to maintain the current timestamp of the process
		mainObj.current_time_stamp = new int[mainObj.num_of_nodes];

		//Initialize all the datastructures needed for the node to run the protocols
		mainObj.initialize(mainObj);

		//Initially node 0 is active therefore if this node is 0 then it should be active
		if(curNode == 0){
			mainObj.active = true;
			////System.out.println("Emitted Messages");			
			//Call Chandy Lamport protocol if it is node 0
			new CLThread(mainObj).start();		
			new EmitMessagesThread(mainObj).start();
		}
		try {
			while (true) {
				// This node listens as a Server for the clients requests 
				Socket socket = listener.accept();
				// For every client request start a new thread 
				new ClientThread(socket,mainObj).start();
			}
		}
		finally {
			listener.close();
		}
	}


	void emitMessages() throws InterruptedException{

		// get a random number between min_per_active to max_per_active to emit that many messages
		int numMsgs = 1;
		int min_send_delay = 0;
		synchronized(this){
			numMsgs = this.getRandomNumber(this.min_per_active,this.max_per_active);
			// If random number is 0 then since node 0 is the only process active in the beginning it will not start
			// therefore get a bigger random number
			if(numMsgs == 0){
				numMsgs = this.getRandomNumber(this.min_per_active + 1,this.max_per_active);
			}
			min_send_delay = this.min_send_delay;
		}
		//System.out.println("For Node "+this.id+ "  Random number of messages in range min - max per active is  "+numMsgs);
		// channels hashMap has all neighbors as keys, store them in an array to get random neighbor
		for(int i=0;i<numMsgs;i++) {
			synchronized(this){
				//get a random number to index in the neighbors and array and get that neighbor
				int neighborIndex = this.getRandomNumber(0,this.neighbors.length-1);
				int curNeighbor = this.neighbors[neighborIndex];
//				System.out.println("Neighbor chosen is "+curNeighbor);
				if(this.active == true){
					//send application message
					ApplicationMsg m = new ApplicationMsg(); 
					// Code for current_time_stamp protocol
					this.current_time_stamp[this.id]++;
					m.current_time_stamp = new int[this.current_time_stamp.length];
					System.arraycopy( this.current_time_stamp, 0, m.current_time_stamp, 0, this.current_time_stamp.length );
					m.nodeId = this.id;
					//					System.out.println("Timestamp that is being sent while message is emitted ");
					//					for(int s:m.current_time_stamp){
					//						System.out.println(s+" ");
					//					}
					// Write the message in the channel connecting to neighbor
					try {
						ObjectOutputStream oos = this.output_stream.get(curNeighbor);
						oos.writeObject(m);	
						oos.flush();
					} catch (IOException e) {
						e.printStackTrace();
					}	
					//increment total_messages_sent
					total_messages_sent++;
				}
			}
			// Wait for minimum sending delay before sending another message
			try {
				Thread.sleep(min_send_delay);
			} catch (InterruptedException e) {
				System.out.println("Error in EmitMessages");
			}
		}
		synchronized(this){
			// After sending min_per_active to max_per_active number of messages become passive
			this.active = false;
		}

	}

	// Function to generate random number in a given range
	int getRandomNumber(int min,int max){
		// Usually this can be a field rather than a method variable
		Random rand = new Random();
		// nextInt is normally exclusive of the top value,
		// so add 1 to make it inclusive
		int randomNum = rand.nextInt((max - min) + 1) + min;
		return randomNum;
	}
}

//Server reading objects sent by other clients in the system in a thread 
class ClientThread extends Thread {
	Socket cSocket;
	ProjectMain mainObj;

	public ClientThread(Socket csocket,ProjectMain mainObj) {
		this.cSocket = csocket;
		this.mainObj = mainObj;
	}

	public void run() {
		ObjectInputStream ois = null;
		try {
			ois = new ObjectInputStream(cSocket.getInputStream());
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		while(true){
			try {
				Message msg;
				msg = (Message) ois.readObject();
				// Synchronizing mainObj so that multiple threads access mainObj in a synchronized way
				synchronized(mainObj){

					//If message is a marker message then process has to turn red if its blue and send messages along all its
					//channels
					if(msg instanceof MarkerMsg){
						int channelNo = ((MarkerMsg) msg).nodeId;
						ChandyLamport.sendMarkerMessage(mainObj,channelNo);
					}	

					//A passive process on receiving an application message only becomes active if 
					//it has sent fewer than max_number messages
					else if((mainObj.active == false) && msg instanceof ApplicationMsg && 
							mainObj.total_messages_sent < mainObj.max_number && mainObj.logging == 0){
						mainObj.active = true; 
						new EmitMessagesThread(mainObj).start();
					}
					//If its an application message and logging = 1 then save it
					else if((mainObj.active == false) && (msg instanceof ApplicationMsg) && (mainObj.logging == 1)){
						//Save the channel No from where the message came from
						int channelNo = ((ApplicationMsg) msg).nodeId;
						//Log the application message since logging is enabled
						ChandyLamport.logMessage(channelNo,((ApplicationMsg) msg) ,mainObj);
					}

					//If message is a state message then if this node id is 0 then process it 
					// otherwise forward it to the parent on converge cast tree towards Node 0
					else if(msg instanceof StateMsg){
						if(mainObj.id == 0){
							//System.out.println("Received State msg at Node 0 from node "+((StateMsg)msg).nodeId);
							mainObj.state_messages.put(((StateMsg)msg).nodeId,((StateMsg)msg));
							mainObj.nodesInGraph[((StateMsg) msg).nodeId] = true;
							//System.out.println("state_messages size = "+mainObj.state_messages.size());
							if(mainObj.state_messages.size() == mainObj.num_of_nodes){
								//System.out.println("State messages are received at node 0");
								boolean restartChandy = ChandyLamport.processstate_messages(mainObj,((StateMsg)msg));
								if(restartChandy){
									//System.out.println("Restarting Chandy Lamport Protocol");
									mainObj.initialize(mainObj);
									//									for(ArrayList<ApplicationMsg> a:mainObj.in_transit_msgs.values()){
									//										System.out.println("Checking if mainObj has empty channel state:"+a.isEmpty());
									//									}
									//Call Chandy Lamport protocol 
									new CLThread(mainObj).start();	
								}								
							}
						}
						else{
							//System.out.println("Forwarding state msg to my parent - node"+mainObj.id);
							ChandyLamport.forwardToParent(mainObj,((StateMsg)msg));
						}
					}
					//If a finishMsg is received then forward the message to all its neighbors
					else if(msg instanceof FinishMsg){	
						//System.out.println("Finish Message of Node "+mainObj.id+" finish message is"+((FinishMsg)msg).msg);
						ChandyLamport.sendFinishMsg(mainObj);
					}

					if(msg instanceof ApplicationMsg){
						//						System.out.println("TimeStamp when application message is received and not processed at node "+mainObj.id);
						//						for(int j: ((ApplicationMsg) msg).current_time_stamp){
						//							System.out.println(j+" ");
						//						}
						//Code for current_time_stamp protocol
						for(int i=0;i<mainObj.num_of_nodes;i++){
							mainObj.current_time_stamp[i] = Math.max(mainObj.current_time_stamp[i], ((ApplicationMsg) msg).current_time_stamp[i]);
						}
						mainObj.current_time_stamp[mainObj.id]++;
						// print the current_time_stamp 
						//						System.out.println("current_time_stamp of node id "+mainObj.id+" when appln msg is received and processed");
						//						for(int i:mainObj.current_time_stamp){
						//							System.out.println(i);
						//						}
					}
				}
			}
			catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}

//Thread to start chandy lamport protocol
class CLThread extends Thread{

	ProjectMain mainObj;
	public CLThread(ProjectMain mainObj){
		this.mainObj = mainObj;
	}
	public void run(){
		//If its the first time calling chandy Lamport protocol, start immediately
		if(mainObj.is_first_time){
			mainObj.is_first_time = false;
		}
		//If its not first time , start after the snapShot delay
		else{
			try {
				Thread.sleep(mainObj.snapshot_delay);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		//Irrespective of first or second time we start the protocol if this thread is started
		ChandyLamport.startSnapshotProtocol(mainObj);
	}
}

//Thread to start chandy lamport protocol
class EmitMessagesThread extends Thread{

	ProjectMain mainObj;
	public EmitMessagesThread(ProjectMain mainObj){
		this.mainObj = mainObj;
	}
	public void run(){
		try {
			mainObj.emitMessages();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
