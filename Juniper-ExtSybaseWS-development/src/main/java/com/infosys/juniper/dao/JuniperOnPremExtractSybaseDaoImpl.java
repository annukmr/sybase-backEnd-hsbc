package com.infosys.juniper.dao;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.crypto.SecretKey;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.infosys.juniper.constant.EncryptionConstants;
import com.infosys.juniper.constant.MetadataDBConstants;
import com.infosys.juniper.dto.ConnectionDto;
import com.infosys.juniper.dto.TempTableInfoDto;
import com.infosys.juniper.dto.TempTableMetadataDto;
import com.infosys.juniper.repository.JuniperOnPremExtractSybaseRepository;
import com.infosys.juniper.util.ConnectionUtils;
import com.infosys.juniper.util.EncryptUtils;

@Component
public class JuniperOnPremExtractSybaseDaoImpl implements JuniperOnPremExtractSybaseDao {
	
	private static final Logger logger = LogManager.getLogger(JuniperOnPremExtractSybaseDaoImpl.class);
	
	@Autowired
	JuniperOnPremExtractSybaseRepository Repository;
	
	private static String master_key_path;
	@SuppressWarnings("static-access")
	@Value("${master.key.path}")
	public void setMasterKeyPath(String value) {
		this.master_key_path=value;
	}
	
	
	@Override
	public String insertSybaseConnectionDetails(Connection conn, ConnectionDto dto) {
		
		int system_sequence=0;
		int project_sequence=0;

		try {
			system_sequence=getSystemSequence(conn,dto.getSystem());
			project_sequence=getProjectSequence(conn,dto.getProject());
			
			System.out.println("system sequence is "+system_sequence+ " project sequence is "+project_sequence);
		}catch(SQLException e) {
			logger.error(e.getMessage());
			return "Error retrieving system and project details";
		}
		String insertConnDetails="";
		String sequence="";
		String connectionId="";
		byte[] encrypted_key=null;
		byte[] encrypted_password=null;
		if(system_sequence!=0 && project_sequence!=0) {
			
			try {
				encrypted_key=getEncryptedKey(conn,system_sequence,project_sequence);
			}catch(Exception e) {
				logger.error(e.getMessage());
				return "Error occured while fetching encryption key"; 
			}
			try {
				encrypted_password=encryptPassword(encrypted_key,dto.getPassword());
			}catch(Exception e) {
				logger.error(e.getMessage());
				return "Error occurred while encrypting password";
			}
			PreparedStatement pstm=null;
			
			try {
				insertConnDetails="insert into "+MetadataDBConstants.CONNECTIONTABLE+
						"(src_conn_name,src_conn_type,host_name,port_no,"
						+ "username,password,encrypted_encr_key,database_name,"
						+ "system_sequence,project_sequence,created_by) "
						+ "values(?,?,?,?,?,?,?,?,?,?,?)";
				pstm = conn.prepareStatement(insertConnDetails);
				pstm.setString(1, dto.getConn_name());
				pstm.setString(2, dto.getConn_type());
				pstm.setString(3, dto.getHostName());
				pstm.setString(4, dto.getPort());
				pstm.setString(5, dto.getUserName());
				pstm.setBytes(6,encrypted_password);
				pstm.setBytes(7,encrypted_key);
				pstm.setString(8, dto.getDbName());
				//pstm.setString(9, dto.getServiceName());
				pstm.setInt(9, system_sequence);
				pstm.setInt(10, project_sequence);
				pstm.setString(11, dto.getJuniper_user());
				pstm.executeUpdate();
				pstm.close();
			}catch(Exception e) {
				logger.error(e.getMessage());
				return e.getMessage();
			}
			
			try {	
				Statement statement=conn.createStatement();
				String query=MetadataDBConstants.GETSEQUENCEID.replace("${tableName}", MetadataDBConstants.CONNECTIONTABLE).replace("${columnName}", MetadataDBConstants.CONNECTIONTABLEKEY);
				ResultSet rs=statement.executeQuery(query);
				if(rs.isBeforeFirst()){
					rs.next();
					sequence=rs.getString(1).split("\\.")[1];
					rs=statement.executeQuery(MetadataDBConstants.GETLASTROWID.replace("${id}", sequence));
					if(rs.isBeforeFirst()){
						rs.next();
						connectionId=rs.getString(1);
					}
				}	
			}catch(Exception e) {
				logger.error(e.getMessage());
				return e.getMessage();
			}finally {
				try {
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					logger.error(e.getMessage());
					return "Execption ocurred while closing connection";
				}
			}

			return "success:"+connectionId;
		}else {
			return "System/Project not found";
		}
	}
	
	
	@Override
	public String updateOracleConnectionDetails(Connection conn, ConnectionDto connDto) {
		String updateConnectionMaster="";
		PreparedStatement pstm=null;
		int system_sequence=0;
		int project_sequence=0;
		byte[] encrypted_key=null;
		byte[] encrypted_password=null;

		try {
			system_sequence=getSystemSequence(conn,connDto.getSystem());
			project_sequence=getProjectSequence(conn,connDto.getProject());
		}catch(SQLException e) {
			logger.error(e.getMessage());
			return "Error while retrieving system or project Details";
		}
		
		if(system_sequence!=0 && project_sequence!=0) {
			
				if(!(connDto.getPassword()==null||connDto.getPassword().isEmpty())) 
				{
					try {
						encrypted_key=getEncryptedKey(conn,system_sequence,project_sequence);
					}catch(Exception e) {
						logger.error(e.getMessage());
						return "Error occured while fetching encryption key"; 
					}
					try {
						encrypted_password=encryptPassword(encrypted_key,connDto.getPassword());
					}catch(Exception e) {
						logger.error(e.getMessage());
						return "Error occurred while encrypting password";
					}					
				}
				else {
					return "Password can not be Null";
				}

				updateConnectionMaster="update "+MetadataDBConstants.CONNECTIONTABLE
						+" set src_conn_name=?"+MetadataDBConstants.COMMA
						+"src_conn_type=?"+MetadataDBConstants.COMMA
						+"host_name=?"+MetadataDBConstants.COMMA
						+"port_no=?"+MetadataDBConstants.COMMA
						+"username=?"+MetadataDBConstants.COMMA
						+"password=?"+MetadataDBConstants.COMMA 
						+"encrypted_encr_key=?"+MetadataDBConstants.COMMA
						+"database_name=?"+MetadataDBConstants.COMMA
						//+"service_name=?"+MetadataDBConstants.COMMA
						+"system_sequence=?"+MetadataDBConstants.COMMA
						+"project_sequence=?"+MetadataDBConstants.COMMA
						+"updated_by=?"
						+" where src_conn_sequence="+connDto.getConnId();
				
				try {	
					pstm = conn.prepareStatement(updateConnectionMaster);
					pstm.setString(1, connDto.getConn_name());
					pstm.setString(2, connDto.getConn_type());
					pstm.setString(3, connDto.getHostName());
					pstm.setString(4, connDto.getPort());
					pstm.setString(5, connDto.getUserName());
					pstm.setBytes(6,encrypted_password);
					pstm.setBytes(7,encrypted_key);
					pstm.setString(8, connDto.getDbName());
					//pstm.setString(9, connDto.getServiceName());
					pstm.setInt(9, system_sequence);
					pstm.setInt(10, project_sequence);
					pstm.setString(11, connDto.getJuniper_user());
					
					pstm.executeUpdate();
					pstm.close();
					return "Success";

				}catch (SQLException e) {

					return e.getMessage();


				}finally {
					try {
						conn.close();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						logger.error(e.getMessage());
						return "Execption ocurred while closing connection";
					}
				}
			

	
			
		}else{
			
			return "System or Project does not Exist";
		}

	}
	
	
	
	
	
	@Override
	public String deleteSybaseConnectionDetails(Connection conn, ConnectionDto connDto) {
		String deleteConnectionMaster="";
		PreparedStatement pstm=null;
		int system_sequence=0;
		int project_sequence=0;
		byte[] encrypted_key=null;
		byte[] encrypted_password=null;

		try {
			system_sequence=getSystemSequence(conn,connDto.getSystem());
			project_sequence=getProjectSequence(conn,connDto.getProject());
		}catch(SQLException e) {
			logger.error(e.getMessage());
			return "Error while retrieving system or project Details";
		}
		
		if(system_sequence!=0 && project_sequence!=0) {
			
				if(!(connDto.getPassword()==null||connDto.getPassword().isEmpty())) 
				{
					try {
						encrypted_key=getEncryptedKey(conn,system_sequence,project_sequence);
					}catch(Exception e) {
						logger.error(e.getMessage());
						return "Error occured while fetching encryption key"; 
					}
					try {
						encrypted_password=encryptPassword(encrypted_key,connDto.getPassword());
					}catch(Exception e) {
						logger.error(e.getMessage());
						return "Error occurred while encrypting password";
					}					
				}
				else {
					return "Password can not be Null";
				}

				deleteConnectionMaster="delete from "+MetadataDBConstants.CONNECTIONTABLE
						+" where src_conn_sequence=?";
//						+" set src_conn_name=?"+MetadataDBConstants.COMMA
//						+"src_conn_type=?"+MetadataDBConstants.COMMA
//						+"host_name=?"+MetadataDBConstants.COMMA
//						+"port_no=?"+MetadataDBConstants.COMMA
//						+"username=?"+MetadataDBConstants.COMMA
//						+"password=?"+MetadataDBConstants.COMMA 
//						+"encrypted_encr_key=?"+MetadataDBConstants.COMMA
//						+"database_name=?"+MetadataDBConstants.COMMA
//						//+"service_name=?"+MetadataDBConstants.COMMA
//						+"system_sequence=?"+MetadataDBConstants.COMMA
//						+"project_sequence=?"+MetadataDBConstants.COMMA
//						+"updated_by=?";
						
				
				try {	
					pstm = conn.prepareStatement(deleteConnectionMaster);
					pstm.setString(1, String.valueOf(connDto.getConnId()));
//					pstm.setString(2, connDto.getConn_type());
//					pstm.setString(3, connDto.getHostName());
//					pstm.setString(4, connDto.getPort());
//					pstm.setString(5, connDto.getUserName());
//					pstm.setBytes(6,encrypted_password);
//					pstm.setBytes(7,encrypted_key);
//					pstm.setString(8, connDto.getDbName());
//					//pstm.setString(9, connDto.getServiceName());
//					pstm.setInt(10, system_sequence);
//					pstm.setInt(11, project_sequence);
//					pstm.setString(12, connDto.getJuniper_user());
					
					pstm.executeUpdate();
					pstm.close();
					return "Success";

				}catch (SQLException e) {

					return e.getMessage();


				}finally {
					try {
						conn.close();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						logger.error(e.getMessage());
						return "Execption ocurred while closing connection";
					}
				}
			

	
			
		}else{
			
			return "System or Project does not Exist";
		}

	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	private int getSystemSequence(Connection conn, String system_name) throws SQLException {
		// TODO Auto-generated method stub
		String query="select system_sequence from "+MetadataDBConstants.SYSTEMTABLE+" where system_name='"+system_name+"'";
		int sys_seq=0;
		Statement statement=conn.createStatement();
		ResultSet rs = statement.executeQuery(query);
		if(rs.isBeforeFirst()) {

			rs.next();
			sys_seq=rs.getInt(1);

		}
		
		return sys_seq;

	}

	private int getProjectSequence(Connection conn, String project) throws SQLException {
		// TODO Auto-generated method stub
		String query="select project_sequence from "+MetadataDBConstants.PROJECTTABLE+" where project_id='"+project+"'";
		int proj_seq=0;
		Statement statement=conn.createStatement();
		ResultSet rs = statement.executeQuery(query);
		if(rs.isBeforeFirst()) {

			rs.next();
			proj_seq=rs.getInt(1);


		}
		
		return proj_seq;
	}
	
	@SuppressWarnings("unchecked")
	private byte[] getEncryptedKey(Connection conn,int system_sequence, int project_sequence) throws Exception {

		
		JSONObject json=new JSONObject();
		json.put("system", Integer.toString(system_sequence));
		json.put("project", Integer.toString(project_sequence));
		
			System.out.println("calling encryption service");
			String response=invokeEncryption(json,EncryptionConstants.ENCRYPTIONSERVICEURL);
			System.out.println("response is "+response);
			JSONObject jsonResponse = (JSONObject) new JSONParser().parse(response);
			if(jsonResponse.get("status").toString().equalsIgnoreCase("FAILED")) {
				throw new Exception("Error ocurred while retrieving encryption key");
			}
			else {

				String query="select key_value from "+MetadataDBConstants.KEYTABLE+" where system_sequence="+system_sequence+
						" and project_sequence="+project_sequence;
				byte[] encrypted_key=null;
				Statement statement=conn.createStatement();
				ResultSet rs = statement.executeQuery(query);
				if(rs.isBeforeFirst()) {

					rs.next();
					encrypted_key=rs.getBytes(1);
					return encrypted_key;

				}
				else {
					throw new Exception("Key not Found");
				}

			}

		}
	
	
	private  String invokeEncryption(JSONObject json,String  url) throws UnsupportedOperationException, Exception {



		CloseableHttpClient httpClient = HttpClientBuilder.create().build();


		HttpPost postRequest=new HttpPost(url);
		postRequest.setHeader("Content-Type","application/json");
		StringEntity input = new StringEntity(json.toString());
		postRequest.setEntity(input); 
		HttpResponse response = httpClient.execute(postRequest);
		HttpEntity respEntity = response.getEntity();
		return EntityUtils.toString(respEntity);
	}
	
	private byte[] encryptPassword(byte[] encrypted_key, String password) throws Exception {

	
			String content = EncryptUtils.readFile(master_key_path);
			SecretKey secKey = EncryptUtils.decodeKeyFromString(content);
			String decrypted_key=EncryptUtils.decryptText(encrypted_key,secKey);
			byte[] encrypted_password=EncryptUtils.encryptText(password,decrypted_key);
			return encrypted_password;


		
	}


	@Override
	public String deleteTempTableMetadata(Connection conn, String feed_id, String src_type) {
		String result="success";
		
		String selectTempTableMaster= "select * from "
				+MetadataDBConstants.TEMPTABLEDETAILSTABLE
				+" where feed_sequence="
				+feed_id;
	
		String deleteTempTableMaster= "delete from "
									+MetadataDBConstants.TEMPTABLEDETAILSTABLE
									+" where feed_sequence="
									+feed_id;
			
		try {	
			Statement statement_selectTemp = conn.createStatement();
			Statement statement_deleteTemp = conn.createStatement();
			ResultSet rs_selectTemp=statement_selectTemp.executeQuery(selectTempTableMaster);
			if(rs_selectTemp.next()) {
				ResultSet rs_deleteTemp=statement_deleteTemp.executeQuery(deleteTempTableMaster);
				if(rs_deleteTemp.next()) {
				result="successfully deleted the temp table records for feed_sequence"+feed_id;
				}else {
					result="Failed to delete the records from the temp table for feed_sequence"+feed_id;
				}
			}
		}catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error(e.getMessage());
			
			result=e.getMessage();


		}finally {
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logger.error(e.getMessage());
			}
		}
		
		return result;	

	}


	@Override
	public String insertTempTableMetadata(Connection conn, TempTableInfoDto tempTableInfoDto) {

		int project_sequence=0;
		Statement statement=null;
		try {
			statement=conn.createStatement();
			project_sequence=getProjectSequence(conn, tempTableInfoDto.getProject());
			if(project_sequence!=0) {
				for(TempTableMetadataDto tempTableMetadata:tempTableInfoDto.getTempTableMetadataArr()) {

					String columns="";
					if(tempTableMetadata.getColumns().equalsIgnoreCase("*")) {
						columns="all";
					}
					else {
						columns=tempTableMetadata.getColumns();
					}
					String insertTableMaster= MetadataDBConstants.INSERTQUERY.replace("{$table}", MetadataDBConstants.TEMPTABLEDETAILSTABLE)
							.replace("{$columns}","feed_sequence,table_name,columns,fetch_type,where_clause,incr_col,view_flag,view_source_schema,project_sequence,created_by" )
							.replace("{$data}",tempTableInfoDto.getFeed_id() +MetadataDBConstants.COMMA
									+MetadataDBConstants.QUOTE+tempTableMetadata.getTable_name()+MetadataDBConstants.QUOTE+MetadataDBConstants.COMMA
									+MetadataDBConstants.QUOTE+columns+MetadataDBConstants.QUOTE+MetadataDBConstants.COMMA
									+MetadataDBConstants.QUOTE+tempTableMetadata.getFetch_type()+MetadataDBConstants.QUOTE+MetadataDBConstants.COMMA
									+MetadataDBConstants.QUOTE+tempTableMetadata.getWhere_clause()+MetadataDBConstants.QUOTE+MetadataDBConstants.COMMA
									+MetadataDBConstants.QUOTE+tempTableMetadata.getIncr_col()+MetadataDBConstants.QUOTE+MetadataDBConstants.COMMA
									+MetadataDBConstants.QUOTE+tempTableMetadata.getView_flag()+MetadataDBConstants.QUOTE+MetadataDBConstants.COMMA
									+MetadataDBConstants.QUOTE+tempTableMetadata.getView_source_schema()+MetadataDBConstants.QUOTE+MetadataDBConstants.COMMA
									+project_sequence+MetadataDBConstants.COMMA
									+MetadataDBConstants.QUOTE+tempTableInfoDto.getJuniper_user()+MetadataDBConstants.QUOTE
									);
					
					statement.executeUpdate(insertTableMaster);
					
				}
				return "success";
				
			}else {
				return "Project Details invalid";
			}
			
		}catch(Exception e) {
			return e.getMessage();
		}finally {
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logger.error(e.getMessage());
			}
		}
		
		

	}


	@Override
	public String metadataValidate(Connection conn, String feed_sequence, String project_id) {
		String connection_type="";
		int counter=0;
		String host="";
		String port="";
		String service_name="";
		String database_name="";
		String user="";
		byte[] encrypted_password=null;
		byte[] encrypted_key=null;
		Connection source_conn=null;
		int count = 0;
		System.out.println("Reached inside validate 3");
		String query="select src_conn_type from "+MetadataDBConstants.CONNECTIONTABLE+
				" where src_conn_sequence=(select distinct src_conn_sequence from "+MetadataDBConstants.FEEDSRCTGTLINKTABLE
				+" where feed_sequence="+feed_sequence+")";
		
		System.out.println(query);
		Statement statement=null;
		
		
		try {
			
			statement=conn.createStatement();
			ResultSet rs=statement.executeQuery(query);
			if(rs.next()) {
				connection_type=rs.getString(1);
				System.out.println("connection_type is "+connection_type);
				if(connection_type.equalsIgnoreCase("SYBASE")) {
					String source_connection_details="select host_name,port_no,username,password,database_name,service_name,ENCRYPTED_ENCR_KEY from "+MetadataDBConstants.CONNECTIONTABLE+
								" where src_conn_sequence=(select distinct src_conn_sequence from "+MetadataDBConstants.FEEDSRCTGTLINKTABLE
								+" where feed_sequence="+feed_sequence+")";
					System.out.println("source_connection_details is "+source_connection_details);
					statement=conn.createStatement();
					ResultSet conn_rs=statement.executeQuery(source_connection_details);
					if(conn_rs.next()) {
						host=conn_rs.getString(1);
						port=conn_rs.getString(2);
						service_name=conn_rs.getString(6);
						database_name=conn_rs.getString(5);
						user=conn_rs.getString(3);
						encrypted_password=conn_rs.getBytes(4);
						encrypted_key=conn_rs.getBytes(7);
						String password=null;
						password = decyptPassword(encrypted_key, encrypted_password);
						String ORACLE_IP_PORT_SID=null;
//						if(service_name!=null) {
							
						if(database_name!=null) {
							System.out.println();
							ORACLE_IP_PORT_SID=host+":"+port+"/"+database_name;
							
						}else {
							ORACLE_IP_PORT_SID=host+":"+port+":"+service_name;
						}
						 
						
						source_conn=ConnectionUtils.connectSybase(ORACLE_IP_PORT_SID, user, password);
						Statement source_conn_statement=source_conn.createStatement();
						String query2="select TABLE_NAME,COLUMNS,WHERE_CLAUSE,INCR_COL from "
									+MetadataDBConstants.TEMPTABLEDETAILSTABLE
									+" where feed_sequence="+feed_sequence 
									+" and project_sequence="
									+"(select project_sequence from JUNIPER_PROJECT_MASTER where project_id='"
									+project_id+"')";
							System.out.println("query2 is "+query2);
							Statement fetch_statement=conn.createStatement();
							ResultSet rs1 = fetch_statement.executeQuery(query2);
							while(rs1.next()) {
								
								count++;
								
								System.out.println("Reached inside the while loop");
								String TABLE_NAME=rs1.getString("TABLE_NAME");
								String COLUMNS=rs1.getString("COLUMNS");
								String WHERE_CLAUSE=rs1.getString("WHERE_CLAUSE");
								String INCR_COL=rs1.getString("INCR_COL");
								if (COLUMNS.equalsIgnoreCase("all") && INCR_COL.equalsIgnoreCase("null")) {
//									query="explain plan for select *"+" from "+TABLE_NAME+" where "+WHERE_CLAUSE;
									query="explain select *"+" from "+TABLE_NAME+" where "+WHERE_CLAUSE;
								}else if (COLUMNS.equalsIgnoreCase("all") && INCR_COL != "null") {
//									query="explain plan for select "+INCR_COL+" from "+TABLE_NAME+" where "+WHERE_CLAUSE;
									query="explain select "+INCR_COL+" from "+TABLE_NAME+" where "+WHERE_CLAUSE;
								}else if (COLUMNS != "all" && INCR_COL.equalsIgnoreCase("null")){
//									query="explain plan for select "+COLUMNS+" from "+TABLE_NAME+" where "+WHERE_CLAUSE;
									query="explain select "+COLUMNS+" from "+TABLE_NAME+" where "+WHERE_CLAUSE;
								}else {
//									query="explain plan for select "+COLUMNS+","+INCR_COL+" from "+TABLE_NAME+" where "+WHERE_CLAUSE;
									query="explain select "+COLUMNS+","+INCR_COL+" from "+TABLE_NAME+" where "+WHERE_CLAUSE;
								}
								
								System.out.println("explain query is "+ query);
								//System.out.println("source_conn_statement.executeQuery(query) : "+source_conn_statement.executeQuery(query));
								System.out.println("Reached inside the validate block 4");
								query = "update "+MetadataDBConstants.TEMPTABLEDETAILSTABLE+" SET VALIDATION_FLAG='Y' where feed_sequence="+feed_sequence+" and TABLE_NAME='"+TABLE_NAME+"' and COLUMNS='"+COLUMNS+"' and INCR_COL='"+INCR_COL+"'";
								System.out.println("query is "+query);
								Statement update_statement=conn.createStatement();
								
								System.out.println("Executing update >>"+ query);
								ResultSet updateStat= update_statement.executeQuery(query);
								System.out.println("Executed update >>"+ query);
								System.out.println("Flag updated successfully for "+TABLE_NAME);
								
								System.out.println("Count >>"+ count);
								
							}
							
							System.out.println("Outside WHILE!");
							
						
					}
					
					if (counter==0) {
						System.out.println("COUNTER=0");
						String delete_main_response=null;
						delete_main_response = deleteTableMetadata(conn,feed_sequence,project_id);
					
						if(delete_main_response.contains("success")) {
							System.out.println("delete_main_response"+ delete_main_response);
								String response=null;
								response = Repository.updateAfterMetadataValidate(feed_sequence,project_id);
								System.out.println("updateAfterMetadataValidate: "+ response);
								if(response.contains("success")) {
									String delete_temp_response=Repository.deleteTempTableMetadata(feed_sequence,project_id);
									
									if(delete_temp_response.contains("success")) {
										System.out.println("delete_temp_response: "+delete_temp_response);
										return "Metadata Validation successfull";		
									}else {
										
										return "The records not deleted from the primary table";
									}
								}else {
									
									return "Data not added in the final table";
								}
						}else{
							return "Failed to remove the records from the juniper_ext_table_master table";
						}
					}else {
						
						return "Metadata Validation failed";
					}
					
					
				}
			
				}
			return null;
		}catch(Exception e) {
			return e.getMessage();
		}finally {
			try {
				conn.close();
				source_conn.close();
			}catch(Exception e) {
				logger.error(e.getMessage());
			}
			
			
		}
		
		
		
	
	}
	
	
	private String decyptPassword(byte[] encrypted_key, byte[] encrypted_password) throws Exception {

		String content = EncryptUtils.readFile(master_key_path);
		SecretKey secKey = EncryptUtils.decodeKeyFromString(content);
		String decrypted_key=EncryptUtils.decryptText(encrypted_key,secKey);
		SecretKey secKey2 = EncryptUtils.decodeKeyFromString(decrypted_key);
		String password=EncryptUtils.decryptText(encrypted_password,secKey2);
		return password;

	}
	
	
	private String deleteTableMetadata(Connection conn,String feed_id,String src_type) throws SQLException{
		String result="success";
		
		
		String selectTableMaster= "select * from "
		+MetadataDBConstants.TABLEDETAILSTABLE
		+" where feed_sequence="
		+feed_id;
		String deleteTableMaster= "delete from "
				+MetadataDBConstants.TABLEDETAILSTABLE
				+" where feed_sequence="
				+feed_id;
									
		try {	
			Statement statement_selectMain = conn.createStatement();
			Statement statement_deleteMain = conn.createStatement();
			
			ResultSet rs_selectMain=statement_selectMain.executeQuery(selectTableMaster);
			if(rs_selectMain.next()) {
				ResultSet rs_deleteMain=statement_deleteMain.executeQuery(deleteTableMaster);
				if(rs_deleteMain.next()) {
				result="successfully deleted the main table records for feed_sequence"+feed_id;
				System.out.println(result);
				}else {
					result="Failed to delete the records from the main table for feed_sequence"+feed_id;
					System.out.println(result);
				}
			}

		}catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error(e.getMessage());
			//TODO: Log the error message
			conn.close();
			result=e.getMessage();


		}
		
		return result;
		}

	


	@Override
	public String updateAfterMetadataValidate(Connection conn,String feed_id,String src_type) {
		String result="";
		String feed_sequence="";
		String TABLE_NAME="";
		String COLUMNS="";
		String WHERE_CLAUSE="";
		String FETCH_TYPE="";
		String INCR_COL="";
		String VIEW_FLAG="";
		String VIEW_SOURCE_SCHEMA="";
		String PROJECT_SEQUENCE="";
		String CREATED_BY="";
		String UPDATED_BY="";
		
					
					String distinctRecords= "select distinct feed_sequence,TABLE_NAME,COLUMNS,WHERE_CLAUSE,FETCH_TYPE,INCR_COL,VIEW_FLAG," + 
							"VIEW_SOURCE_SCHEMA,PROJECT_SEQUENCE,CREATED_BY,UPDATED_BY " + 
							"from JUNIPER_EXT_TABLE_MASTER_TEMP " + 
							"where feed_sequence="+feed_id +
							" and validation_flag ='Y'";
		
												
					try {	
						Statement statement = conn.createStatement();
						Statement statement2 = conn.createStatement();
						ResultSet rs=statement.executeQuery(distinctRecords);	
						while(rs.next()) {
							feed_sequence=rs.getString("feed_sequence");
							TABLE_NAME=rs.getString("TABLE_NAME");			
							COLUMNS=rs.getString("COLUMNS");
							WHERE_CLAUSE=rs.getString("WHERE_CLAUSE");
							FETCH_TYPE=rs.getString("FETCH_TYPE");
							INCR_COL=rs.getString("INCR_COL");
							VIEW_FLAG=rs.getString("VIEW_FLAG");
							VIEW_SOURCE_SCHEMA=rs.getString("VIEW_SOURCE_SCHEMA");
							PROJECT_SEQUENCE=rs.getString("PROJECT_SEQUENCE");
							CREATED_BY=rs.getString("CREATED_BY");
							UPDATED_BY=rs.getString("UPDATED_BY");	
							String insertRecords ="insert into JUNIPER_EXT_TABLE_MASTER (feed_sequence,TABLE_NAME,COLUMNS,WHERE_CLAUSE,FETCH_TYPE,INCR_COL,VIEW_FLAG,VIEW_SOURCE_SCHEMA,PROJECT_SEQUENCE,CREATED_BY,UPDATED_BY)"
									+"values("+feed_sequence
									+",'"+TABLE_NAME+"','"+COLUMNS+"','"+WHERE_CLAUSE+"','"+FETCH_TYPE
									+"','"+INCR_COL
									+"','"+VIEW_FLAG
									+"','"+VIEW_SOURCE_SCHEMA
									+"',"+PROJECT_SEQUENCE
									+",'"+CREATED_BY
									+"','"+UPDATED_BY
									+"')";
							try {
								statement2.executeQuery(insertRecords);
							}catch(SQLException e) {
								result=e.getMessage();
							}
						}
						result="Records inserted in the main table successfully";

					}catch (SQLException e) {
						logger.error(e.getMessage());
			
						
						result=e.getMessage();
					}finally {
						try {
							conn.close();
						} catch (SQLException e) {
							logger.error(e.getMessage());
						}
					}
					return result;	
		}

	


}
