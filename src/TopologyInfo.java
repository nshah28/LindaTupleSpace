import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Created by manishah on 4/24/17.
 */
public class TopologyInfo {

    serverDetails currentHost;

    public Map<Integer, List<Integer>> getNodeToPartition() {
        return NodeToPartition;
    }

    public List<Integer> getPartitionToNode() {
        return PartitionToNode;
    }

    public void setNodeToPartition(Map<Integer, List<Integer>> nodeToPartition) {
        NodeToPartition = nodeToPartition;
    }

    public void setPartitionToNode(List<Integer> partitionToNode) {
        PartitionToNode = partitionToNode;
    }

    private Map<Integer, List<Integer>> NodeToPartition = new TreeMap<>();
    final int range = ((int) Math.pow(2, 28));
    //final int noOfPartitions = Integer.MAX_VALUE / range;
    final int noOfPartitions = (int)((Integer.MAX_VALUE + 1L) / range);
    private List<Integer> PartitionToNode = new ArrayList<Integer>();

    public TopologyInfo(serverDetails currentHost) {
        this.currentHost = currentHost;
        this.currentHost.setTopologyInfo(this);
        //init(currentHost);
    }

    public void init(serverDetails host) {
        List<Integer> partitions = new ArrayList<Integer>();
        NodeToPartition.put(host.getHostID(), partitions);
        for(int i=0; i<noOfPartitions; i++) {
            PartitionToNode.add(i, host.getHostID());
            partitions.add(i,i);
        }
    }

    public List<Integer> recalculatePartitions(serverDetails newHost) {
        int avgPartitionsPerHost = noOfPartitions / (NodeToPartition.size() + 1);
        List<Integer> newHostPartitions = new ArrayList<Integer>();
        if (avgPartitionsPerHost == noOfPartitions) {
            // bootstrap case
            init(newHost);
            return newHostPartitions;
        }
        List<Integer> partitionsToBeGivenFromCurrentHost = new ArrayList<Integer>();
        for (Map.Entry<Integer, List<Integer>> nTp : NodeToPartition.entrySet()) {
            List<Integer> partitions = nTp.getValue();
            int host = nTp.getKey();
            if (partitions.size() > avgPartitionsPerHost) {
                while (partitions.size() != avgPartitionsPerHost) {
                    // remove extra partition from existing node
                    int partition = partitions.remove(avgPartitionsPerHost);
                    if (host == currentHost.getHostID()) {
                        partitionsToBeGivenFromCurrentHost.add(partition);
                    }
                    // add this partition to new node
                    newHostPartitions.add(partition);
                    // change mapping for partition to node
                    PartitionToNode.set(partition, newHost.getHostID());
                }
            }
        }
        // Update the new host to partitions in topology
        NodeToPartition.put(newHost.getHostID(), newHostPartitions);
        return partitionsToBeGivenFromCurrentHost;
    }

    public void convertInputMapToListOfPartitionsToHost() {
        PartitionToNode = new ArrayList<>(noOfPartitions); // form a list of all partitions
        //Assign a right host to right partition index based on the map
        for (Map.Entry<Integer, List<Integer>> nTp : NodeToPartition.entrySet()) {
            int host = nTp.getKey();
            for (int partition: nTp.getValue()) {
                PartitionToNode.set(partition, host);
            }
        }
    }

    public void convertInputListOfPartitionsToHostToAMap() {
        NodeToPartition.clear();
        for (int i = 0; i < noOfPartitions; i++) {
            int host = PartitionToNode.get(i);
            if (NodeToPartition.containsKey(host)) {
                // add partition index to host
                NodeToPartition.get(host).add(i);
            } else {
                List<Integer> partitions = new ArrayList<Integer>();
                partitions.add(i);
                NodeToPartition.put(host, partitions);
            }
        }
    }

    public void printNodeToPartitions() {
        System.out.println("Node to partitions is: ");
        for (Map.Entry<Integer, List<Integer>> nTp : NodeToPartition.entrySet()) {
            List<Integer> partitions = nTp.getValue();
            Integer host = nTp.getKey();
            System.out.println("Host: " + host + "\t" + "Partitions: " + partitions.toString());
        }
    }

    public void printPartitionsToNode() {
        System.out.println("Partitions to Node is: ");
        for (int i=0; i < PartitionToNode.size(); i++) {
            System.out.println("Partition: " + i + "\t" + "Node: " + PartitionToNode.get(i));
        }
    }

    public List<Integer> addNewHost(serverDetails newHost) {
        List<Integer> partitionsToBeCopied = recalculatePartitions(newHost);
        //copyPartitions(partitionsToBeCopied);
        for (int i=0; i < partitionsToBeCopied.size(); i++) {
            System.out.println("partitionsToBeCopied: " +partitionsToBeCopied.get(i));
        }
        return partitionsToBeCopied;
    }

    // It will update the current topology info held by this node, and return null.
    // If it's a node to be deleted, it will return following:
    // Map of [Neighbour_Host_id -> List of Partitions to be Tranfered from deleted node to this node]
    public Map<Integer, List<Integer>> redistributePartitions(serverDetails deleteHost) throws Exception {

        if (!NodeToPartition.containsKey(deleteHost.getHostID())) {
            throw new Exception("Deleted host does not exists in Topology");
        }

        // By default assume a new node is being
        int noOfNodesLeftInSystem = NodeToPartition.size() - 1; // Once we delete current node

        if (noOfNodesLeftInSystem < 0) {
            throw new Exception("No nodes in the system so cannot delete anything");
        }

        System.out.println("noOfNodesLeftInSystem "+noOfNodesLeftInSystem);
        if (noOfNodesLeftInSystem == 0) {
            // Nothing left in the System so clear all maps
            PartitionToNode.clear();
            NodeToPartition.clear();
            // Nothing to be transferred so return empty map
            return new HashMap<Integer, List<Integer>>();
        }

        int avgPartitionsPerHost = (int) Math.round((noOfPartitions * 1.0d) / noOfNodesLeftInSystem);
        // Remove the deleted node from the map of NodeToPartitions as it no longer exists
        List<Integer> partitionsHeldByDeletedNode = NodeToPartition.remove(deleteHost.getHostID());

        // To keep a track of what needs to be distributed
        Map<Integer, List<Integer>> partitionsToBeDistributedMap = new HashMap<Integer, List<Integer>>();

        // Update the existing maps to accommodate the delete
        for (Map.Entry<Integer, List<Integer>> nTp : NodeToPartition.entrySet()) {
            if (partitionsHeldByDeletedNode.isEmpty()) {
                break;
            }
            List<Integer> currenPartitions = nTp.getValue(); // These are partitions held currently by this host
            int host = nTp.getKey();
            List<Integer> newPartitions; // These are partitions to be transferred to this node due to delete
            if (currenPartitions.size() <= avgPartitionsPerHost) { // This means some partitions need to be given to this host
                if (partitionsToBeDistributedMap.containsKey(host)) {
                    newPartitions = partitionsToBeDistributedMap.get(host);
                } else {
                    newPartitions = new ArrayList<Integer>();
                    partitionsToBeDistributedMap.put(host, newPartitions);
                }
                while (currenPartitions.size() <= avgPartitionsPerHost && !partitionsHeldByDeletedNode.isEmpty()) {
                    // remove one partition from deleted node and give it to this host
                    int partition = partitionsHeldByDeletedNode.remove(0);
                    currenPartitions.add(partition); // This will automatically update in the NodeToPartition map
                    PartitionToNode.set(partition, host);
                    newPartitions.add(partition); // update map of partitions to be distributed
                }
            }
        }

        if (deleteHost.getHostID() == currentHost.getHostID()) {
            // This code is executing on the node which is to be deleted
            return partitionsToBeDistributedMap;
        } else {

            // This code is executing on a node which is not deleted
            // Here we don't care about partitionsToBeDistributedMap and just updating
            // it's own topology is enough so we can simply return null

            return partitionsToBeDistributedMap; /// you can return a null here but for printing purposes kept as is.
        }
    }

    //accept hostID as a parameter to delete and if request is currenthost then returns a list HostID-partitionList to distribute
    public Map<Integer, List<Integer>> deleteHost(serverDetails deleteHost) throws Exception {
        Map<Integer, List<Integer>> partitionsToBeCopied = redistributePartitions(deleteHost);
        if (partitionsToBeCopied != null) {
            System.out.println("Partitions to be copied to different hosts: ");
            printInputMapOfNodeToPartitions(partitionsToBeCopied);
            // copy partitions to different hosts
        }

        return partitionsToBeCopied;
    }

    private void printInputMapOfNodeToPartitions(Map<Integer, List<Integer>> map) {
        for (Map.Entry<Integer, List<Integer>> nTp : map.entrySet()) {
            List<Integer> partitions = nTp.getValue();
            Integer host = nTp.getKey();
            System.out.println("Host: " + host + "\t" + "Partitions: " + partitions.toString());
        }
    }

}
