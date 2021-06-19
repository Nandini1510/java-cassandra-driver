import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import java.net.InetSocketAddress;
import java.util.Random;
import java.util.UUID;
public class getCassClusterInfo {
    
    public static void main(String args[]){
        
        try{
            cassConnectionManager cass=new cassConnectionManager();
            cass.connectDB();
            CqlSession session=cass.getSession();
            Row r1=session.execute(SimpleStatement.builder("SELECT cluster_name, release_version FROM system.local").build()).one();
            System.out.println("Connected to "+ r1.getString("cluster_name")+ " and it is running "+r1.getString("release_version")+" version.");
            System.out.println("-------------------------------------------------------");
            Row r2=session.execute(SimpleStatement.builder("SELECT count(1) AS nodes_count FROM system.peers").build()).one();
            System.out.println("This cluster contains "+ (int)r2.getLong("nodes_count")+ " nodes.");
            System.out.println("-------------------------------------------------------");
            System.out.println("Done");
            session.close();
        }catch(Exception e){
            System.out.println(e);
        }
    }
}
