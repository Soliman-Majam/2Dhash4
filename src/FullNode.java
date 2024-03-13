// IN2011 Computer Networks
// Coursework 2023/2024
//
// Submission by
// Soliman Majam
// 220003523
// Soliman.Majam@city.ac.uk


// DO NOT EDIT starts
interface FullNodeInterface {
    public boolean listen(String ipAddress, int portNumber);
    public void handleIncomingConnections(String startingNodeName, String startingNodeAddress);
}
// DO NOT EDIT ends


public class FullNode implements FullNodeInterface {

    String name = "Soliman.Majam@city.ac.uk:FirstNewFullNodeTest,1.0";
    String address = "86.4.90.124:127.0.0.1:2244";

    public boolean listen(String ipAddress, int portNumber) {
	// Implement this!
	// Return true if the node can accept incoming connections
	// Return false otherwise
	return true;
    }
    
    public void handleIncomingConnections(String startingNodeName, String startingNodeAddress) {
	// Implement this!
	return;
    }
}
