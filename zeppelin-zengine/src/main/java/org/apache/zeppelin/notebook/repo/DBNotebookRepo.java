package org.apache.zeppelin.notebook.repo;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.NoteInfo;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.scheduler.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.*;

/**
 *
 */

public class DBNotebookRepo implements NotebookRepo {
  Logger logger = LoggerFactory.getLogger(DBNotebookRepo.class);

  private String DB_SCHEMA = "dash104750";
  private String USERNAME = "dash104750";
  private String PASSWORD = "HwK8U4gUlIrg";
  private String DRIVER = "COM.ibm.db2os390.sqlj.jdbc.DB2SQLJDriver";
  private String JDBCURL = "jdbc:db2://awh-yp-small02.services.dal.bluemix.net:50000/BLUDB";

  private StringBuilder sb;

  private Connection con = null;

  public DBNotebookRepo(ZeppelinConfiguration conf) {
    try {
      logger.info("Connecting to Database !!");
      Class.forName(DRIVER);
      Properties connectionProps = new Properties();
      connectionProps.put("user", USERNAME);
      connectionProps.put("password", PASSWORD);
      connectionProps.put("allowNextOnExhaustedResultSet", 1);
      con = DriverManager.getConnection(JDBCURL, connectionProps);
      logger.info("Connected to Database !!" + con.toString());
    }
    catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    catch (SQLException sqle)  {
      logger.error("EXIT: SQLException getConnection, Message:" +
              sqle.getMessage() + " - " + sqle.getErrorCode());
    }
  }

  @Override
  public List<NoteInfo> list() throws IOException {

    logger.info("Entered list call");
    List<NoteInfo> infos = new LinkedList<NoteInfo>();

    String query = "SELECT NOTEBOOK_ID FROM " + DB_SCHEMA + ".NOTEBOOKCONTENT ";


    try {
      PreparedStatement ps1 = con.prepareStatement(query);
      ResultSet result = ps1.executeQuery();

      while (result.next()) {
       // Note note = getNote(result.getString("NOTEBOOK_ID"));
        NoteInfo info = new NoteInfo(result.getString("NOTEBOOK_ID"),
                "", new HashMap<String, Object>());
        if (info != null) {
          infos.add(info);
        }
      }

    } catch (SQLException sqle) {
      logger.error("EXIT: SQLException getConnection, Message:" +
              sqle.getMessage() + " - " + sqle.getErrorCode());
      sqle.printStackTrace();
    }

    logger.info("left list call");
    return infos;
  }

  @Override
  public Note get(String noteId) throws IOException {

    logger.info("Entered get call " + noteId);
    return getNote(noteId);

  }

  private Note getNote (String noteId) {

    logger.info("Entered getNote" + noteId);
    Note note = new Note();

    if (con == null) {
      logger.error("DB connection failed");
      return null;
    }
    String query1 = "SELECT NOTEBOOK_ID, NOTEBOOK_DATA FROM " + DB_SCHEMA +
            ".NOTEBOOKCONTENT WHERE NOTEBOOK_ID = ?";
    try {
      PreparedStatement ps1 = con.prepareStatement(query1);
      ps1.setString(1, noteId);
      Clob clob =  null;
      ResultSet result = ps1.executeQuery();
      if (result.next()) {
        clob = result.getClob(2);
      }
      GsonBuilder gsonBuilder = new GsonBuilder();
      gsonBuilder.setPrettyPrinting();
      Gson gson = gsonBuilder.create();


      if (clob != null) {
        note = gson.fromJson(clob.getSubString(1, (int) clob.length()), Note.class);
      }
      for (Paragraph p : note.getParagraphs()) {
        if (p.getStatus() == Job.Status.PENDING || p.getStatus() == Job.Status.RUNNING) {
          p.setStatus(Job.Status.ABORT);
        }
      }
      logger.info("left getNote");
    } catch (SQLException sqle) {
      logger.error("EXIT: SQLException getConnection, Message:" +
              sqle.getMessage() + " - " + sqle.getErrorCode());
      sqle.printStackTrace();
    }

    return note;

  }

  @Override
  public void save(Note note) throws IOException {
    GsonBuilder gsonBuilder = new GsonBuilder();
    gsonBuilder.setPrettyPrinting();
    Gson gson = gsonBuilder.create();
    String json = gson.toJson(note);
    PreparedStatement ps;
    sb = new StringBuilder();

    String mergeQuery = sb.append("MERGE INTO ")
            .append(DB_SCHEMA)
            .append(".NOTEBOOKCONTENT AS NBC USING ")
            .append(" (VALUES (?,?)) AS NBC_TMP (NOTEBOOK_ID, NOTEBOOK_DATA) ON ")
            .append(" NBC.NOTEBOOK_ID = NBC_TMP.NOTEBOOK_ID ")
            .append(" WHEN MATCHED THEN ")
            .append(" UPDATE SET NBC.NOTEBOOK_DATA = NBC_TMP.NOTEBOOK_DATA ")
            .append(" WHEN NOT MATCHED THEN ")
            .append(" INSERT (NOTEBOOK_ID, NOTEBOOK_DATA) ")
            .append(" VALUES (NBC_TMP.NOTEBOOK_ID, NBC_TMP.NOTEBOOK_DATA) ")
            .append(" ELSE IGNORE").toString();

    try {
      ps = con.prepareStatement(mergeQuery);
      ps.setString(1, note.getId());
      ps.setString(2, json);
      if (ps.executeUpdate() == 1) {
        logger.info(" Record merged successfully for Notebookd Id: " + note.getId());
      }

    } catch (SQLException sqle) {
      logger.error("EXIT: SQLException getConnection, Message:" +
              sqle.getMessage() + " - " + sqle.getErrorCode());
      sqle.printStackTrace();
    }


  }

  @Override
  public void remove(String noteId) throws IOException {

    PreparedStatement ps;

    String checkQuery = "DELETE FROM " + DB_SCHEMA +
            ".NOTEBOOKCONTENT WHERE NOTEBOOK_ID = ?";
    try {
      ps = con.prepareStatement(checkQuery);
      ps.setString(1, noteId);
      if (ps.executeUpdate() == 1) {
        logger.info("Notebook " + noteId + " deleted successfully");
      }

    } catch (SQLException sqle) {
      logger.error("EXIT: SQLException getConnection, Message:" +
              sqle.getMessage() + " - " + sqle.getErrorCode());
      sqle.printStackTrace();
    }

  }

  @Override
  public void close() {
    //no-op
  }

  @Override
  public void checkpoint(String noteId, String checkPointName) throws IOException {
    // no-op
    logger.info("Checkpoint feature isn't supported in {}", this.getClass().toString());
  }
}
