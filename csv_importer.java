import java.io.*;
import java.sql.*;
import java.text.*;
import java.util.Calendar;
import java.util.Date;
public class csv_importer {	
	public static String path =  new File("").getAbsolutePath();
	public static String log_path;
	/*DATABASE*/
	public static String jdbcDriver = "com.mysql.jdbc.Driver";
    public static String db_url = "jdbc:mysql://192.168.1.160:3306/mass?autoReconnect=true&useSSL=false";
    public static String userPass = "?user=root&password=";
    public static String dbName = "mass";
    public static String username = "root";
    public static String password = "Alethe@123";
    public static PreparedStatement preStatement;
    public static Statement st;
    public static ResultSet rs;
    public static Connection conn;
    public static String query;
   
    public static Calendar cal = Calendar.getInstance();
    public static Date system_date_java = new Date();					
	public static java.sql.Timestamp system_date_sql = new java.sql.Timestamp(system_date_java.getTime());;
	public static DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    public static DateFormat tf = new SimpleDateFormat("HHmmss");
    
    
    public static void main(String[] args) throws IOException{   	
    	
    	/*Database connection*/
    	log_printer(path, "Connecting to database - JDBC URL:"+db_url+"; User Name:" + username +"; Passowrd:" + password);  //System.out.println("Connecting database...");
    	try {
    		Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://192.168.1.160/mass", "root", "Alethe@123");
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
    	log_printer(path, "Database Connected!!");
    	
		log_printer(path, "Running");
        String csvFile1 = "/home/gaurav/Desktop/pipetally_our.csv";
        BufferedReader br1 = null;
        String line1 = "";       
        String cvsSplitBy = ",";
        
        int pig_bid =0;
		String CD_NO =null;
		int page_num =0;
		int samp_5000 =0 ;
		int abs_val =0;
		int score =0; 
		String feature_name =null;
		double odo_distance =0.0;
		String o_clock =null ;
		double marker_score =0.0;
		double sleeve_score =0.0;
		double flange_score =0.0;

        try {
            br1 = new BufferedReader(new FileReader(csvFile1));
            while ((line1 = br1.readLine()) != null){
                // use comma as separator
                String[] row1 = line1.split(cvsSplitBy);
                
                 pig_bid = Integer.parseInt(row1[0]); 
                 CD_NO = row1[1];
                 page_num = Integer.parseInt(row1[2]);
        		 samp_5000 = Integer.parseInt(row1[3]);
        		 abs_val = Integer.parseInt(row1[4]);
        		 score = Integer.parseInt(row1[5]);
        		 feature_name = row1[6];
        		 odo_distance = Double.valueOf(row1[7]);
        		 o_clock = row1[8];
        		 marker_score = Double.valueOf(row1[9]);
        		 sleeve_score = Double.valueOf(row1[10]);
        		 flange_score = Double.valueOf(row1[11]);
                
                insert_funct(pig_bid, CD_NO, page_num, samp_5000, abs_val, score, feature_name, odo_distance, o_clock,marker_score, sleeve_score, flange_score);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br1 != null) {
                try {
                    br1.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
		System.out.println("Done");

    }    
    public static void insert_funct( int pig_bid, String CD_NO, int page_num, int samp_5000, int abs_val, int score,
    		                         String feature_name, double odo_distance, String o_clock, double marker_score,
    		                         double sleeve_score, double flange_score) throws IOException{
		
    query = "INSERT INTO pipetally_new (pig_bid, CD_NO, page_num, samp_5000, abs_val, score, feature_name, odo_distance, o_clock, marker_score, sleeve_score, flange_score) VALUES ("
    			+ "?,?,?,?,?,?,?,?,?,?,?,?);"; 
	    try{    
	    PreparedStatement preStatement = conn.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
	    
	    System.out.println("here");
	    
		preStatement.setInt(1, pig_bid);
		preStatement.setString(2, CD_NO);
		preStatement.setInt(3, page_num);
		preStatement.setInt(4, samp_5000);
		preStatement.setInt(5, abs_val);
		preStatement.setInt(6, score);
		preStatement.setString(7, feature_name);
		preStatement.setDouble(8, odo_distance);
		preStatement.setString(9, o_clock);
		preStatement.setDouble(10, marker_score);
		preStatement.setDouble(11, sleeve_score);
		preStatement.setDouble(12, flange_score);
		//System.out.println("first set");

		preStatement.executeUpdate();
		
		System.out.println("There");
		 
		ResultSet rs = preStatement.getGeneratedKeys();
		int generatedKey = 0;
		if (rs.next()) {
			generatedKey = rs.getInt(1);
		}	
		log_printer(path, "Inserted record's ID:	" + generatedKey);
	}catch(SQLException e){log_printer(path, "exc  :"+ e.getMessage()); e.printStackTrace();}
    }
    	 public static void log_printer(String path, String message) throws IOException
 	    {
 		 	String sfileName =  tf.format(system_date_java)+"_dataImporter.txt";
 	    	String str_date = df.format(system_date_java);
 	    	new File(path + "/Crawler_log_files/" + str_date+"/"+"pipetally").mkdirs();
 	    	log_path = path + "/Crawler_log_files/" + str_date+"/"+"pipetally";
 	    	File myFile = new File(log_path + "/" +sfileName);
 			//myFile.createNewFile();		// if file already exists will do nothing 
 			FileWriter fw = new FileWriter(myFile.getAbsoluteFile(),true);
 			BufferedWriter bw = new BufferedWriter(fw);
 			SimpleDateFormat tmsf = new SimpleDateFormat("HH:mm:ss.SSS");
 			Date curr_date = new Date();
 			bw.write(tmsf.format(curr_date)+"\t"+ message);
 			bw.newLine();
 			bw.close();	 	
 	    }
}
