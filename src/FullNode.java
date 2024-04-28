import java.io.*;
import java.net.*;
import java.sql.SQLOutput;
import java.util.*;

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

    // comprised of email and unique string
    private String name;
    //comprised of IP address and port number
    private String address;
    //hash map for storing other nodes' address
    private Map<String, String> networkMap;
    private Writer writer;
    private BufferedReader in;
    private ServerSocket server;
    private Socket clientSocket;
    private String startingNodeName, startingNodeAddress;

    public boolean listen(String ipAddress, int portNumber) {
        try {
            // set node address
            this.address = ipAddress + ":" + portNumber;

            // listen for any connections with new ServerSocket
            server = new ServerSocket(portNumber);
            System.out.println("listened successfully");
            server.close();

            return true;

        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public void handleIncomingConnections(String startingNodeName, String startingNodeAddress) {
        try {
            System.out.println("Handling connection..");
            // name of node
            this.name = "Soliman.Majam@city.ac.uk:FirstNewFullNodeTest,1.0";

            this.startingNodeName = startingNodeName;
            this.startingNodeAddress = startingNodeAddress;

            // connect to node nad let other nodes know
            // address made up of ip address and port number
            String[] parts = startingNodeAddress.split(":");
            String ipAddress = parts[0];

            if (parts.length != 2) {
                System.err.println("Invalid startingNodeAddress format: " + startingNodeAddress);
                return;
            }

            int portNumber = Integer.parseInt(parts[1]);
            server = new ServerSocket();
            server.bind(new InetSocketAddress(ipAddress, portNumber));
            System.out.println("Server socket created");
            clientSocket = server.accept();
            System.out.println("Client socket created");
            writer = new OutputStreamWriter(clientSocket.getOutputStream());
            in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            // START message
            serverWrite("START 1 " + this.name);

            networkMap = new HashMap<>();

            // wait until receives 'START' response
            String response = in.readLine();
            if (response != null && response.startsWith("START ")) {
                System.out.println("Message accepted");

                System.out.println("Connection established, ready for incoming requests: " + '\n');
                // sned 'NOTIFY' request
                serverWrite("NOTIFY");
                serverWrite(this.name);
                serverWrite(this.address);

                requestHandler();
            }

            // end connection
            clientSocket.close();
            server.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void requestHandler() {
        try {
            String line;
            while ((line = in.readLine()) != null) {
                System.out.println("This is the current line being read: " + line);

                // if 'PUT?' request then
                if (line.startsWith("PUT? ")) {
                    // read values and keys
                    String[] parts = line.split(" ");
                    if (parts.length == 3) {
                        int keyLines = Integer.parseInt(parts[1]);
                        int valueLines = Integer.parseInt(parts[2]);

                        StringBuilder keyBuilder = new StringBuilder();
                        StringBuilder valueBuilder = new StringBuilder();

                        for (int i = 0; i < keyLines; i++) {
                            keyBuilder.append(in.readLine()).append("\n");
                        }
                        for (int i = 0; i < valueLines; i++) {
                            valueBuilder.append(in.readLine()).append("\n");
                        }

                        String key = keyBuilder.toString();
                        String value = valueBuilder.toString();


                        // compute hashID for the value to be stored
                        String valueHashID;
                        try {
                            valueHashID = HashID.computeHashID(value);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }

                        // check networkMap for if valueHashID is one of the closest nodes
                        boolean isClosest = isClosestNode(valueHashID);

                        if (isClosest) {
                            // check if 3 nodes don't already have the same distance
                            addToMap(key, value);
                            serverWrite("SUCCESS");

                        } else {
                            // "FAILED" as this node is not one of three closest nodes
                            serverWrite("FAILED");
                        }
                    } else {
                        serverWrite("INVALID");
                        System.out.println("Invalid PUT? request: " + line);
                        serverWrite("END");
                    }

                } else if (line.startsWith("GET? ")) {
                    System.out.println(line);
                    String[] parts = line.split(" ");
                    if (parts.length == 2) {
                        int keyLines = Integer.parseInt(parts[1]);

                        StringBuilder keyBuilder = new StringBuilder();

                        for (int i = 0; i < keyLines; i++) {
                            keyBuilder.append(in.readLine()).append("\n");
                        }
                        String key = keyBuilder.toString(); // trim to remove newline

                        // check if key exists in network map
                        if (networkMap.containsKey(key)) {
                            // if true get key's value
                            String value = networkMap.get(key);

                            // split value into lines to count number of lines
                            String[] valueLines = value.split("\n");

                            // send 'VALUE' and the number of lines
                            serverWrite("VALUE " + valueLines.length);
                            serverWrite(value); // then the actual value
                        } else {
                            // if not send 'NOPE'
                            serverWrite("NOPE");
                        }
                    } else {
                        serverWrite("INVALID");
                        System.out.println("Invalid GET? request: " + line);
                        serverWrite("END");
                    }
                }
                else if (line.startsWith("NEAREST?")) {
                    // get hashID from request
                    String[] parts = line.split(" ");
                    if (parts.length == 2) {
                        String hashID = parts[1];

                        // find three closest nodes to given hashID
                        List<String> closestNodes = findClosestNodes(hashID);

                        // send NODES response
                        serverWrite("NODES " + closestNodes.size());
                        for (String node : closestNodes) {
                            String[] nodeParts = node.split(":");
                            serverWrite(nodeParts[0]); // node name
                            serverWrite(nodeParts[1]); // node address
                        }
                    } else {
                        // invalid GET? request format
                        serverWrite("INVALID");
                        System.out.println("Invalid NEAREST? request: " + line);
                        serverWrite("END");
                        return;
                    }
                } else if (line.equals("ECHO?")) {
                    // send reverse
                    serverWrite("OHCE");

                } else if (line.equals("NOTIFIED")) {
                    // if response successful, initialize that network hash map and add the connected node to it
                    networkMap.put(startingNodeName, startingNodeAddress);
                    System.out.println("Network map updated");
                }else if (line.equals("END")) {
                    serverWrite("END");
                    return;
                }
                 else {
                    // invalid request
                    serverWrite("INVALID");
                    System.out.println("Invalid Request: " + line);
                    serverWrite("END");
                    return;
                }
            }

        }   catch (Exception e) {
            e.printStackTrace();
        }
    }


    private boolean isClosestNode(String valueHashID) {
        // get the distances between valueHashID and hashIDs of nodes in the network map
        List<Integer> distances = new ArrayList<>();
        for (String nodeHashID : networkMap.keySet()) {
            // method calculates distance between two strings, adds the distance calculated to distances
            distances.add(calculateDistance(nodeHashID, valueHashID));
        }

        // sort distances in ascending order
        Collections.sort(distances);

        // check for closest three nodes
        int count = 0;
        for (int distance : distances) {
            if (distance == 0) {
                // skip
                continue;
            }
            if (count >= 3) {
                // done
                break;
            }
            if (distance <= getCurrentNodeDistanceThreshold()) {
                // node that is close enough has been found
                count++;
            }
        }

        // if count is less than 3, current node is one of three closest nodes
        return count < 3;
    }

    private int calculateDistance(String hashID1, String hashID2) {
        int distance = 0;
        for (int i = 0; i < hashID1.length(); i++) {
            // bitwise XOR of hex chars
            int xorResult = Character.digit(hashID1.charAt(i), 16) ^ Character.digit(hashID2.charAt(i), 16);

            // count leading matching 0's
            int leadingZeros = Integer.numberOfLeadingZeros(xorResult);

            // compute distance based on formula (256 - matching leading bits)
            distance += 256 - leadingZeros;
        }
        return distance;
    }

    private int getCurrentNodeDistanceThreshold() {
        // i don't knwo what the threshold would be
        return 100;
    }

    public void addToMap(String key, String value) throws Exception {
        int count = 0; // keeps track of amount of nodes with same distance
        String valueHashID = HashID.computeHashID(value); // calculates hash value of value

        // start for loop iterating over the values... hashing them.. then commparing distance.. for every value
        for (Map.Entry<String, String> entry : networkMap.entrySet()) {
            String existingValue = entry.getValue();
            String existingValueHashID = HashID.computeHashID(existingValue + '\n');

            // calculate distance between HashID of current value and new value
            int distance = calculateDistance(existingValueHashID, valueHashID);

            // if distance is 0 means they're the same, increment count
            if (distance == 0) {
                count++;

                // if count == then 3 nodes have the same distance already, exit method without adding the key, value pair
                if (count >= 3) {
                    System.out.println("Three nodes with same distance already present");
                    System.out.println("FAILED");
                    return;
                }
            }
        }

        // if all pairs have been checked and count is less than 3, add the new key, value pair
        networkMap.put(key, value);
        System.out.println("SUCCESS");
    }

    private boolean serverWrite(String message) {

        try {
            writer.write(message + '\n');
            System.out.println(name + ": " + message);
            writer.flush();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    private List<String> findClosestNodes(String hashID) {
        List<String> closestNodes = new ArrayList<>();
        Map<Integer, List<String>> distances = new HashMap<>();

        // calculate distance between given hashID and evevery node in network map
        for (Map.Entry<String, String> entry : networkMap.entrySet()) {
            String nodeName = entry.getKey();
            int distance = calculateDistance(hashID, nodeName);

            // add node to corresponding distance list
            List<String> nodeList = distances.computeIfAbsent(distance, k -> new ArrayList<>());
            nodeList.add(nodeName + ":" + entry.getValue()); // store node name and address together
        }

        // sort distance map by distance ascending
        List<Integer> sortedDistances = new ArrayList<>(distances.keySet());
        Collections.sort(sortedDistances);

        // add first three unique nodes to closestNodes list
        int count = 0;
        for (int distance : sortedDistances) {
            List<String> nodes = distances.get(distance);
            for (String node : nodes) {
                if (!closestNodes.contains(node)) {
                    closestNodes.add(node);
                    count++;
                    if (count == 3) {
                        return closestNodes;
                    }
                }
            }
        }
        return closestNodes;
    }

    public static void main(String[] args) {
        FullNode a = new FullNode();
        if (a.listen("127.0.0.1", 20000)) {
            a.handleIncomingConnections("Soliman.Majam@city.ac.uk:FirstNewFullNodeTest,1.0", "127.0.0.1:20000");
        }
    }
}
