
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import java.util.Random;
import java.util.UUID;

public class SalesApp_GenerateUsers {

    static ConsistencyLevel CASS_READ_CONSISTENCY = ConsistencyLevel.ONE;
    static ConsistencyLevel CASS_WRITE_CONSISTENCY = ConsistencyLevel.ONE;

    public static void main(String[] args) {
        //reading number of users through command line arguments
        if (args.length < 2) {
            System.out.println("ERROR : Missing Input. Requires 1 Number Input.");
            System.exit(1);
        }
        int input_var = 0;
        try {
            input_var = Integer.parseInt(args[1]);
        } catch (Exception e) {
            System.out.println(e.toString() + "ERROR : Requires 1 Number Input.");
        }
        //connecting to database
        cassConnectionManager cass = new cassConnectionManager();
        cass.connectDB();
        CqlSession session = cass.getSession();
        int v_number_of_users = 0;
        try {
            //preparing statements
            PreparedStatement cql_email_stmt = session.prepare(
                    "SELECT email_server FROM lookup_email_servers WHERE id = ?");
            PreparedStatement cql_state_stmt = session.prepare(
                    "SELECT state_code FROM lookup_usa_states WHERE id = ?");
            PreparedStatement cql_platform_stmt = session.prepare(
                    "SELECT platform FROM lookup_user_platforms WHERE id = ?");
            PreparedStatement cql_user_insert = session.prepare(
                    "INSERT INTO users (user_id, user_name, user_email_id, user_state_code, user_phone_number, "
                    + "user_platform) VALUES (?, ?, ?, ?, ?, ?)");

            for (int user_id = 1; user_id <= input_var; user_id++) {
                //generate a random id and pick random vales from lookup tables
                int id = new Random().nextInt(10 - 1) + 1;

                ResultSet email_output = session.execute(cql_email_stmt.bind(id));
                Row cass_row = email_output.one();
                String var_email_server = "";
                if (cass_row != null) {
                    var_email_server = cass_row.getString("email_server");
                } else {
                    System.out.println("No email_server for id: " + id);
                    session.close();
                    System.exit(1);
                }
                
                id = new Random().nextInt(51 - 1) + 1;
                ResultSet state_output = session.execute(cql_state_stmt.bind(id));
                cass_row = state_output.one();
                String var_state_code = "";
                if (cass_row != null) {
                    var_state_code = cass_row.getString("state_code");
                } else {
                    System.out.println("No state_code for id: " + id);
                    session.close();
                    System.exit(1);
                }
                
                id = new Random().nextInt(10 - 1) + 1;
                ResultSet platform_output = session.execute(cql_platform_stmt.bind(id));
                cass_row = platform_output.one();
                String var_platform = "";
                if (cass_row != null) {
                    var_platform = cass_row.getString("platform");
                } else {
                    System.out.println("No platform field for id: " + id);
                    session.close();
                    System.exit(1);
                }
                //generating username, email and phone number
                String var_user_name = UUID.randomUUID().toString().substring(1, 13).replace("-", "");
                String var_user_email_id = var_user_name + var_email_server;
                String var_user_phone_number = String.valueOf(new Random().nextInt(900 - 700) + 700)
                        + "-" + String.valueOf(new Random().nextInt(900 - 100) + 100)
                        + "-" + String.valueOf(new Random().nextInt(9999 - 1001) + 1001);
                
                //inserting data into the user tables
                session.execute(cql_user_insert.bind((long) user_id, var_user_name, var_user_email_id,
                        var_state_code, var_user_phone_number, var_platform));
                v_number_of_users = user_id;
            }  //end of for loop

        } //end of try
        catch (Exception e) {
            System.out.println(e.getStackTrace());
            session.close();
        }
        System.out.println(v_number_of_users + " users generated. \n Done");

        session.close();
    }

}
