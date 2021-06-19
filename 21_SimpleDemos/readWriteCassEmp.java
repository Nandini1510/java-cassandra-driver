
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import java.net.InetSocketAddress;
import java.util.Random;
import java.util.UUID;

public class readWriteCassEmp {

    static ConsistencyLevel CASS_READ_CONSISTENCY = ConsistencyLevel.ONE;
    static ConsistencyLevel CASS_WRITE_CONSISTENCY = ConsistencyLevel.ONE;

    public static void main(String args[]) {

        try {
            cassConnectionManager cass = new cassConnectionManager();
            cass.connectDB();
            CqlSession session = cass.getSession();
            SimpleStatementBuilder s1 = SimpleStatement.builder("INSERT INTO emp (empid, first_name, last_name) VALUES (?, ?, ?)");
            s1.setConsistencyLevel(CASS_WRITE_CONSISTENCY);
            int emp_id = new Random().nextInt(9999 - 1111) + 1111;
            String first_name = UUID.randomUUID().toString().substring(1, 13).replace("-", "");
            String last_name = UUID.randomUUID().toString().substring(1, 13).replace("-", "");

            session.execute(s1.addPositionalValues(emp_id, first_name, last_name).build());
            System.out.println("---- 1 row inserted -----------------------------------");
            System.out.println("-------------------------------------------------------");
            System.out.println("empid | first_name | last_name");
            System.out.println("-------------------------------------------------------");

            SimpleStatementBuilder s2 = SimpleStatement.builder("SELECT empid, first_name, last_name FROM emp WHERE empid = ?");
            s2.setConsistencyLevel(CASS_READ_CONSISTENCY);
            Row r = session.execute(s2.addPositionalValue(1001).build()).one();
            System.out.println(r.getInt("empid") + "|" + r.getString("first_name") + "|" + r.getString("last_name"));
            System.out.println("-------------------------------------------------------");
            System.out.println("---- select and print all rows --------------------------");
            System.out.println("-------------------------------------------------------");
            System.out.println("empid | first_name | last_name");
            System.out.println("-------------------------------------------------------");
            SimpleStatementBuilder s3 = SimpleStatement.builder("SELECT empid, first_name, last_name FROM emp LIMIT 5");
            s3.setConsistencyLevel(CASS_READ_CONSISTENCY);
            ResultSet rs = session.execute(s3.build());
            for (Row r1 : rs) {
                System.out.println(r1.getInt("empid") + "|" + r1.getString("first_name") + "|" + r1.getString("last_name"));
            }
            System.out.println("-------------------------------------------------------");
            System.out.println("Done");
            session.close();
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
