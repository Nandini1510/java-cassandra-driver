
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.ConsistencyLevel;

import java.util.Random;
import java.util.UUID;
import java.math.BigDecimal;
import java.math.RoundingMode;

public class SalesApp_GenerateProducts {

    static ConsistencyLevel CASS_READ_CONSISTENCY = ConsistencyLevel.ONE;
    static ConsistencyLevel CASS_WRITE_CONSISTENCY = ConsistencyLevel.ONE;
    
    public static void main(String[] args) {
        //reading number of products through command line arguments
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
        
        int v_number_of_products = 0;
        try {
            //preparing statements
            PreparedStatement cql_prdcat_stmt = session.prepare("SELECT product_category FROM lookup_product_categories WHERE id = ?");
            PreparedStatement cql_product_insert = session.prepare(
                    "INSERT INTO products (product_id, product_code, product_name, product_description, "
                    + "product_category, product_price, product_qoh) VALUES (?, ?, ?, ?, ?, ?, ?)");
            
            for (int var_product_id = 1; var_product_id <= input_var; var_product_id++) {
                //generate a random id and pick random values from lookup table
                int id = new Random().nextInt(20 - 1) + 1;
                
                BoundStatement bound = cql_prdcat_stmt.bind(id);
                ResultSet rs = session.execute(bound);
                Row cass_row = rs.one();
                String var_product_category = "";
                if (cass_row != null) {
                    var_product_category = cass_row.getString("product_category");
                }
                else {

                    System.out.println("No product found for id: " + id);
                    session.close();
                    System.exit(1);
                }
                //generating product details
                String var_product_code = UUID.randomUUID().toString().substring(1, 13).replace("-", "");
                
                String var_product_name = UUID.randomUUID().toString()
                        .substring(1, new Random().nextInt(9 - 5) + 5).replace("-", "")
                        + UUID.randomUUID().toString()
                                .substring(1, new Random().nextInt(9 - 5) + 5).replace("-", "");
                
                String var_product_description = UUID.randomUUID().toString()
                        .substring(1, new Random().nextInt(6 - 5) + 5).replace("-", "")
                        + UUID.randomUUID().toString()
                                .substring(1, new Random().nextInt(9 - 6) + 6).replace("-", "")
                        + UUID.randomUUID().toString()
                                .substring(1, new Random().nextInt(5 - 3) + 3).replace("-", "")
                        + UUID.randomUUID().toString()
                                .substring(1, new Random().nextInt(11 - 7) + 7).replace("-", "");

                double price = new Random().nextInt(60 - 10) + 10
                        + (new Random().nextInt(99 - 0) / 100.00);
                BigDecimal var_product_price = new BigDecimal(price);

                long var_product_qoh = new Random().nextInt(5555 - 555) + 555;
                
                //inserting data into products table
                session.execute(cql_product_insert.bind((long) var_product_id, var_product_code, var_product_name,
                        var_product_description, var_product_category, var_product_price.setScale(2, RoundingMode.UP), var_product_qoh));
                v_number_of_products = var_product_id;
            }
        } catch (Exception e) {

            System.out.println(e);
            session.close();
        }
        System.out.println(v_number_of_products + " products generated. \n Done");
        session.close();
    }

}
