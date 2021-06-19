
import com.datastax.oss.driver.api.core.CqlSession;
import java.net.InetSocketAddress;

public class cassConnectionManager {

    CqlSession cs;

    public void connectDB() {
        this.cs = CqlSession.builder()
                .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
                .withAuthCredentials("cassandra", "cassandra")
                .withLocalDatacenter("datacenter1")
                .withKeyspace("sales")
                .build();
    }

    public CqlSession getSession() {
        return this.cs;
    }

    public void close() {
        cs.close();
    }
}
