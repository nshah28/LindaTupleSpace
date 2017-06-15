/**
 * Created by nidhi on 4/24/17.
 */
public class serverDetails {
    private int hostID;
    private String hostName;
    private String ip;
    private int port;
    private TopologyInfo topologyInfo;
    private serverDetails backuphost;

    public serverDetails(String hostName, String ip, int port) {
        this.hostName = hostName;
        this.ip = ip;
        this.port = port;
    }

    public serverDetails(int hostID, String hostName, String ip, int port) {
        this.hostID = hostID;
        this.hostName = hostName;
        this.ip = ip;
        this.port = port;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public int getHostID() {
        return hostID;
    }

    public void setHostID(int id) {
        this.hostID = id;
    }

    public void setTopologyInfo(TopologyInfo topologyInfo) {
        this.topologyInfo = topologyInfo;
    }

    public void setBackuphost(serverDetails backuphost) {
        this.backuphost = backuphost;
    }

    public TopologyInfo getTopologyInfo() {
        return topologyInfo;
    }

    public serverDetails getBackuphost() {
        return backuphost;
    }
}