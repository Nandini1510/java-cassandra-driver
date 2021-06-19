
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.ConsistencyLevel;

import java.time.LocalDate;
import java.util.Random;
import java.util.UUID;
import java.time.LocalDateTime;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class SalesApp_GenerateOrders {

    static ConsistencyLevel CASS_READ_CONSISTENCY = ConsistencyLevel.ONE;
    static ConsistencyLevel CASS_WRITE_CONSISTENCY = ConsistencyLevel.ONE;

    public static void main(String[] args) {

        int v_number_of_orders = 0;
        cassConnectionManager cass = new cassConnectionManager();
        cass.connectDB();
        CqlSession session = cass.getSession();
        int var_users_count;
        int var_products_count;
        //counting the number of users and products from tables
        ResultSet users_count = session.execute("SELECT count(1) as user_rec_count FROM users;");
        Row user_row = users_count.one();
        var_users_count = (int) user_row.getLong("user_rec_count");

        ResultSet products_count = session.execute("SELECT count(1) as prod_rec_count FROM products;");
        Row product_row = products_count.one();
        var_products_count = (int) product_row.getLong("prod_rec_count");
        
        System.out.println(var_users_count + " " + var_products_count);
        
        //preparing statements
        PreparedStatement cql_user_stmt = session.prepare(
                "SELECT user_id, user_email_id, user_name, user_phone_number, user_platform, user_state_code FROM users WHERE user_id = ?");

        PreparedStatement cql_product_stmt = session.prepare(
                "SELECT product_id, product_category, product_code, product_name, product_price, product_qoh FROM products WHERE product_id = ?");

        PreparedStatement cql_order_insert = session.prepare(
                "INSERT INTO sales_orders (order_date, order_date_hour, order_timestamp, order_code, order_discount_percent, order_estimated_shipping_date, order_grand_total, order_number_of_products, order_total, user_email_id, user_id, user_name, user_phone_number, user_platform, user_state_code) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

        PreparedStatement cql_order_products_insert = session.prepare(
                "INSERT INTO sales_order_products (order_date, order_code, product_id, product_category, product_code, product_name, product_price_each, product_price_total, product_sold_quantity) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");

        PreparedStatement cql_product_qoh_insert = session.prepare(
                "INSERT INTO products (product_id, product_qoh) VALUES (?, ?)");
        
        //generating random number of orders
        int orders_count = new Random().nextInt(22 - 4) + 4;
        for (int var_orders = 1; var_orders <= orders_count; var_orders++) {

            //retrieving user details
            BoundStatement cql_stmt = cql_user_stmt.bind((long) new Random().nextInt(var_users_count - 1) + 1);
            ResultSet user_output = session.execute(cql_stmt);
            Row cass_row = user_output.one();
            if (cass_row == null) {
                System.out.println("Could not extract record from users table.");
                session.close();
                System.exit(1);
            }
            long var_user_id = cass_row.getLong("user_id");
            String var_user_email_id = cass_row.getString("user_email_id");
            String var_user_name = cass_row.getString("user_name");
            String var_user_phone_number = cass_row.getString("user_phone_number");
            String var_user_platform = cass_row.getString("user_platform");
            String var_user_state_code = cass_row.getString("user_state_code");

            //generating order details
            LocalDate var_order_date = LocalDate.now();
            Instant var_order_timestamp = Instant.now();
            int var_order_date_hour = LocalDateTime.now().getHour();
            UUID var_order_code = UUID.randomUUID();
            int var_order_discount_percent = new Random().nextInt(5 - 0);
            Instant var_order_estimated_shipping_date = Instant.now().plus((long) new Random().nextInt(20 - 3) + 3, ChronoUnit.DAYS);
            BigDecimal var_order_grand_total;
            int var_order_number_of_products = 0;
            BigDecimal var_order_total = new BigDecimal(0);
            int product_count = new Random().nextInt(8 - 2) + 2;
            for (int var_order_products = 1; var_order_products < product_count; var_order_products++) {
                
                //retrieving product details
                cql_stmt = cql_product_stmt.bind((long) new Random().nextInt(var_products_count - 1) + 1);
                ResultSet product_output = session.execute(cql_stmt);
                cass_row = product_output.one();
                if (cass_row == null) {
                    System.out.println("Could not extract record from products table.");
                    session.close();
                    System.exit(1);
                }
                
                long var_product_id = cass_row.getLong("product_id");
                String var_product_category = cass_row.getString("product_category");
                String var_product_code = cass_row.getString("product_code");
                String var_product_name = cass_row.getString("product_name");
                BigDecimal var_product_price = cass_row.getBigDecimal("product_price");
                long var_product_qoh = cass_row.getLong("product_qoh");
                
                int var_product_sold_quantity = new Random().nextInt(15 - 3) + 3;
                BigDecimal var_product_price_total
                        = BigDecimal.valueOf(var_product_price.floatValue() * var_product_sold_quantity);
                //checking if we have enough product on-hand
                if (var_product_qoh > (var_product_sold_quantity + 50)) {
                    //inserting into order_products table
                    cql_stmt = cql_order_products_insert.bind(var_order_date, var_order_code, var_product_id, var_product_category, var_product_code, var_product_name, var_product_price, var_product_price_total, var_product_sold_quantity);
                    session.execute(cql_stmt);
                    //updating product quantity and inserting into product table
                    cql_stmt = cql_product_qoh_insert.bind(var_product_id, var_product_qoh - var_product_sold_quantity);
                    session.execute(cql_stmt);
                    var_order_number_of_products = var_order_number_of_products + 1;
                    var_order_total = BigDecimal.valueOf(var_order_total.floatValue() + var_product_price_total.floatValue());
                }
            }
            var_order_grand_total = BigDecimal.valueOf(var_order_total.floatValue()
                    - ((var_order_total.floatValue() * var_order_discount_percent) / 100.00));

            if (var_order_number_of_products > 0) {
                //inserting into orders table only when number of products ordered is more than zero
                cql_stmt = cql_order_insert.bind(var_order_date, var_order_date_hour, var_order_timestamp, var_order_code, var_order_discount_percent, var_order_estimated_shipping_date, var_order_grand_total, var_order_number_of_products, var_order_total, var_user_email_id, var_user_id, var_user_name, var_user_phone_number, var_user_platform, var_user_state_code);
                session.execute(cql_stmt);
                v_number_of_orders = v_number_of_orders + 1;
            }
        }
        session.close();
    }
}
