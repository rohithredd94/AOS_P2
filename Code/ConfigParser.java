import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class ConfigParser {

	public static ProjectMain readConfigFile(String name) throws IOException{
		ProjectMain mySystem = new ProjectMain();
		int count = 0,flag = 0;
		// Variable to keep track of the current node whose neighbors are being updated
		int curNode = 0;
		// The name of the file to open.
		String curDir = System.getProperty("user.dir");
		String fileName = curDir+"/"+name;
		// This will reference one line at a time
		String line = null;
		try {
			// FileReader reads text files in the default encoding.
			FileReader fileReader = new FileReader(fileName);
			// Always wrap FileReader in BufferedReader.
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			while((line = bufferedReader.readLine()) != null) {
				if(line.length() == 0)
					continue;
				// Ignore comments and consider only those lines which are not comments
				if(!line.startsWith("#")){
					if(line.contains("#")){
						String[] input = line.split("#.*$");
						String[] input1 = input[0].split("\\s+");
						if(flag == 0 && input1.length == 6){
							mySystem.num_of_nodes = Integer.parseInt(input1[0]);
							mySystem.min_per_active = Integer.parseInt(input1[1]);
							mySystem.max_per_active = Integer.parseInt(input1[2]);
							mySystem.min_send_delay = Integer.parseInt(input1[3]);
							mySystem.snapshot_delay = Integer.parseInt(input1[4]);
							mySystem.max_number = Integer.parseInt(input1[5]);
							flag++;
							mySystem.adj_matrix = new int[mySystem.num_of_nodes][mySystem.num_of_nodes];
						}
						else if(flag == 1 && count < mySystem.num_of_nodes)
						{							
							mySystem.nodes.add(new Node(Integer.parseInt(input1[0]),input1[1],Integer.parseInt(input1[2])));
							count++;
							if(count == mySystem.num_of_nodes){
								flag = 2;
							}
						}
						else if(flag == 2){
							insertIntoMatrix(input1,mySystem, curNode);
							curNode++;
						}
					}
					else {
						String[] input = line.split("\\s+");
						if(flag == 0 && input.length == 6){
							mySystem.num_of_nodes = Integer.parseInt(input[0]);
							mySystem.min_per_active = Integer.parseInt(input[1]);
							mySystem.max_per_active = Integer.parseInt(input[2]);
							mySystem.min_send_delay = Integer.parseInt(input[3]);
							mySystem.snapshot_delay = Integer.parseInt(input[4]);
							mySystem.max_number = Integer.parseInt(input[5]);
							flag++;
							mySystem.adj_matrix = new int[mySystem.num_of_nodes][mySystem.num_of_nodes];
						}
						else if(flag == 1 && count < mySystem.num_of_nodes)
						{
							mySystem.nodes.add(new Node(Integer.parseInt(input[0]),input[1],Integer.parseInt(input[2])));
							count++;
							if(count == mySystem.num_of_nodes){
								flag = 2;
							}
						}
						else if(flag == 2){
							insertIntoMatrix(input,mySystem,curNode);
							curNode++;
						}
					}
				}
			}
			// Always close files.
			bufferedReader.close();  
		}
		catch(FileNotFoundException ex) {
			System.out.println("Unable to open file '" +fileName + "'");                
		}
		catch(IOException ex) {
			System.out.println("Error reading file '" + fileName + "'");                  
		}
		for(int i=0;i<mySystem.num_of_nodes;i++){
			for(int j=0;j<mySystem.num_of_nodes;j++){
				if(mySystem.adj_matrix[i][j] == 1){
					mySystem.adj_matrix[j][i] = 1;
				}
			}
		}
		return mySystem;
	}

	static void insertIntoMatrix(String[] input, ProjectMain mySystem,int curNode) {
		for(String i:input){
			mySystem.adj_matrix[curNode][Integer.parseInt(i)] = 1;
		}
	}

//	public static void main(String[] args) throws IOException{
//		ProjectMain m = ConfigParser.readConfigFile("config1.txt");
//		for(int i=0;i<m.num_of_nodes;i++){
//			for(int j=0;j<m.num_of_nodes;j++){
//				System.out.print(m.adj_matrix[i][j]+"  ");
//			}
//			System.out.println();
//		}
//
//	}
}

