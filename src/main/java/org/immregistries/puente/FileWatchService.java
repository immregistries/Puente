package org.immregistries.puente;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.text.StringSubstitutor;
import org.immregistries.mqe.hl7util.SeverityLevel;
import org.immregistries.mqe.validator.detection.ValidationReport;
import org.immregistries.mqe.validator.engine.MessageValidator;
import org.immregistries.mqe.validator.engine.ValidationRuleResult;
import org.immregistries.mqe.vxu.MqeAddress;
import org.immregistries.mqe.vxu.MqeMessageHeader;
import org.immregistries.mqe.vxu.MqeMessageReceived;
import org.immregistries.mqe.vxu.MqePatient;
import org.immregistries.mqe.vxu.MqeVaccination;

public class FileWatchService {
  private final WatchService watcher;
  private final Map<WatchKey, Path> keys;
  private MessageValidator validator = MessageValidator.INSTANCE;

  private static final String DIR_SEND = "send";
  private static final String DIR_SEND_ERROR = "send-error";
  private static final String DIR_SEND_READY = "send-ready";
  private static final String DIR_REQUEST = "request";

  private final String vxuTemplate =
      "MSH|^~\\&|||||${messageHeaderDate}||VXU^V04^VXU_V04||P|2.5.1|||ER|AL|||||Z22^CDCPHINVS\n"
      + "PID|1||U09J28375^^^AIRA-TEST^MR||${lastName}^${firstName}^${middleName}^^^^L||${birthDate}|${sex}||2106-3^White^CDCREC|${street}^${street2}^${city}^${state}^${zipCode}^USA^P|||||||||||\n"
      + "RXA|0|1|${administrationDate}||${administeredCode}|999|||01^Historical information - source unspecified^NIP001|||||||||||CP|A";

  private final String vxuTemplateNew = "MSH|^~\\&|||||${messageHeaderDate}||VXU^V04^VXU_V04|J69O9.9l|P|2.5.1|||ER|AL|||||Z22^CDCPHINVS|\r" + 
      "PID|1||J69O9^^^AIRA-TEST^MR||${lastName}^${firstName}^${middleName}^^^^L|BanderaAIRA^StephanyAIRA^^^^^M|${birthDate}|${sex}||2054-5^Black or African-American^CDCREC|${street}^${street2}^${city}^${state}^${zipCode}^USA^P||^PRN^PH^^^734^9473420|||||||||2186-5^not Hispanic or Latino^CDCREC|\r" + 
      "PD1|||||||||||02^Reminder/Recall - any method^HL70215|||||A|20201214|20201214|\r" + 
      "ORC|RE||J69O9.3^AIRA|\r" + 
      "RXA|0|1|${administrationDate}||${administeredCode}|999|||01^Historical^NIP001||||||||MSD^Merck and Co^MVX|||CP|A|\r";
  
  FileWatchService(Path dir) throws IOException {
    this.watcher = FileSystems.getDefault().newWatchService();
    this.keys = new HashMap<WatchKey, Path>();

    registerDirectory(dir);
  }

  private void registerDirectory(Path dir) throws IOException {
    WatchKey key = dir.register(watcher, ENTRY_CREATE, ENTRY_MODIFY);
    keys.put(key, dir);
  }

  void processEvents() throws IOException {
    for (; ; ) {
      WatchKey key;
      try {
        key = watcher.take();
      } catch (InterruptedException ie) {
        return;
      }

      Path dir = keys.get(key);
      if (dir == null) {
        continue;
      }

      for (WatchEvent<?> event : key.pollEvents()) {
        @SuppressWarnings("rawtypes")
        WatchEvent.Kind kind = event.kind();

        @SuppressWarnings("unchecked")
        Path name = ((WatchEvent<Path>) event).context();
        Path child = dir.resolve(name);

        System.out.format("%s: %s\n", event.kind().name(), child);
        evaluateFile(child.toFile());
      }

      boolean valid = key.reset();
      if (!key.reset()) {
        keys.remove(key);

        // all directories are inaccessible
        if (keys.isEmpty()) {
          break;
        }
      }
    }
  }

  void evaluateFile(File file) throws IOException {
    System.out.println("Evaluating File");
    File errorFile = null;
    File readyFile = null;
    Iterable<CSVRecord> records = new ArrayList<CSVRecord>();
    try {
      records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(new FileReader(file));
    } catch (Exception e) {
      System.out.println("Couldn't parse file.");
    }
    for (CSVRecord record : records) {
      System.out.println(record);

      try {
        String vaccinationEventId = record.get("Vaccination event ID");
        String recipientId = record.get("Recipient ID");
        String firstName = record.get("Recipient name: first");
        String middleName = record.get("Recipient name: middle");
        String lastName = record.get("Recipient name: last");
        String birthDate = record.get("Recipient date of birth");
        String sex = record.get("Recipient sex");
        String street = record.get("Recipient address: street");
        String street2 = record.get("Recipient address: street 2");
        String city = record.get("Recipient address: city");
        String county = record.get("Recipient address: county");
        String state = record.get("Recipient address: state");
        String zipCode = record.get("Recipient address: zip code");
        String administrationDate = record.get("Administration date");
        String cvx = record.get("CVX");
        String ndc = record.get("NDC");
        String mvx = record.get("MVX");
        String lotNumber = record.get("Lot number");
        String vaccineExpDate = record.get("Vaccine expiration date");
        String vaccineAdmSite = record.get("Vaccine administering site");
        String vaccineRoute = record.get("Vaccine route of administration");
        String responsibleOrg = record.get("Responsible organization");
        String admAtLoc = record.get("Administered at location");

        MqeMessageReceived mmr = new MqeMessageReceived();
        MqeMessageHeader header = mmr.getMessageHeader();
        MqePatient patient = mmr.getPatient();
        MqeAddress address = patient.getPatientAddress();
        List<MqeVaccination> vaccinations = mmr.getVaccinations();
        MqeVaccination vaccination = new MqeVaccination();

        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmssZ");
        Date date = new Date(System.currentTimeMillis());
        header.setMessageDateString(formatter.format(date));
        header.setMessageDate(date);

        vaccination.setAdminDateString(administrationDate);
        vaccination.setAdminCvxCode(cvx);
        vaccination.setAdminNdcCode(ndc);
        vaccination.setManufacturerCode(mvx);
        vaccination.setLotNumber(lotNumber);
        vaccination.setExpirationDateString(vaccineExpDate);
        vaccination.setBodySiteCode(vaccineAdmSite);
        vaccination.setBodyRouteCode(vaccineRoute);
        vaccination.setActionCode("A");
        vaccinations.add(vaccination);

        address.setStreet(street);
        address.setStreet2(street2);
        address.setCity(city);
        address.setStateCode(state);
        address.setZip(zipCode);

        patient.setNameFirst(firstName);
        patient.setNameMiddle(middleName);
        patient.setNameLast(lastName);
        patient.setBirthDateString(birthDate);
        patient.setSexCode(sex);
        patient.setIdSubmitterNumber(recipientId);

        List<ValidationRuleResult> list = validator.validateMessage(mmr);
        boolean cleanFile = reportResults(list);
        if (!cleanFile) {
          errorFile = writeErrorFile(list, record, file.getName(), errorFile);
        } else {
          Map<String, String> valuesMap = new HashMap<>();
          valuesMap.put("messageHeaderDate", header.getMessageDateString());
          valuesMap.put("lastName", lastName);
          valuesMap.put("firstName", firstName);
          valuesMap.put("middleName", middleName);
          valuesMap.put("birthDate", birthDate);
          valuesMap.put("sex", sex);
          valuesMap.put("street", street);
          valuesMap.put("street2", street2);
          valuesMap.put("street2", street2);
          valuesMap.put("city", city);
          valuesMap.put("state", state);
          valuesMap.put("zipCode", zipCode);
          valuesMap.put("administrationDate", administrationDate);
          String administeredCode = "";
          if (cvx != null) {
            administeredCode = cvx + "^^CVX";
          } else if (ndc != null) {
            administeredCode = ndc + "^^NDC";
          }
          valuesMap.put("administeredCode", administeredCode);
          StringSubstitutor sub = new StringSubstitutor(valuesMap);
          String resolvedString = sub.replace(vxuTemplate);
          System.out.println(resolvedString);
          writeFile(file.getName(), resolvedString);
          readyFile = writeReadyFile(list, record, file.getName(), readyFile);
        }
        file.delete();
    } catch (IllegalArgumentException iae) {
      String message = iae.getMessage().split(",")[0];
      System.out.println(message);
    }
      }
  }

  File writeReadyFile(List<ValidationRuleResult> list, CSVRecord record, String name, File file)
      throws IOException {
    String directoryName = "./" + DIR_SEND + "/" + DIR_SEND_READY;

    File directory = new File(directoryName);
    if (!directory.exists()) {
      directory.mkdir();
    }

    if (file == null) {
      SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
      Date date = new Date(System.currentTimeMillis());
      String dateStr = formatter.format(date);
      String fileName = name.split("\\.")[0] + "-READY-" + dateStr + ".csv";
      file = new File(directoryName + "/" + fileName);
    }

    FileWriter fw = new FileWriter(file.getAbsoluteFile());
    BufferedWriter bw = new BufferedWriter(fw);
    bw.write(record.toString());
    bw.close();

    return file;
  }

  File writeErrorFile(List<ValidationRuleResult> list, CSVRecord record, String name, File file)
      throws IOException {
    String directoryName = "./" + DIR_SEND + "/" + DIR_SEND_ERROR;

    File directory = new File(directoryName);
    if (!directory.exists()) {
      directory.mkdir();
    }

    if (file == null) {
      SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
      Date date = new Date(System.currentTimeMillis());
      String dateStr = formatter.format(date);
      String fileName = name.split("\\.")[0] + "-ERROR-" + dateStr + ".csv";
      file = new File(directoryName + "/" + fileName);
    }

    FileWriter fw = new FileWriter(file.getAbsoluteFile());
    BufferedWriter bw = new BufferedWriter(fw);
    String recordString = "";
    Iterator<String> itr = record.iterator();
    while (itr.hasNext()) {
      recordString += itr.next();
      if (itr.hasNext()) {
        recordString += ",";
      }
    }
    bw.write(recordString);
    bw.close();

    return file;
  }

  void writeFile(String name, String value) throws IOException {
    String directoryName = "./" + DIR_REQUEST;
    System.out.println("Writing file");
    SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
    Date date = new Date(System.currentTimeMillis());
    String dateStr = formatter.format(date);
    String fileName = name.split("\\.")[0] + "-" + dateStr + ".hl7";

    File directory = new File(directoryName);
    if (!directory.exists()) {
      directory.mkdir();
    }

    File file = new File(directoryName + "/" + fileName);
    FileWriter fw = new FileWriter(file.getAbsoluteFile());
    BufferedWriter bw = new BufferedWriter(fw);
    bw.write(value);
    bw.close();
  }

  private boolean reportResults(List<ValidationRuleResult> list) {
    for (ValidationRuleResult vrr : list) {
      for (ValidationReport i : vrr.getValidationDetections()) {
        if (SeverityLevel.ERROR == i.getSeverity()) {
          String s = "  - ";
          if (i.getHl7LocationList() != null && i.getHl7LocationList().size() > 0) {
            s += i.getHl7LocationList().get(0);
          }
          s += "                   ";
          if (s.length() > 10) {
            s = s.substring(0, 18);
          }
          System.out.println(s + ": " + i.getDetection() + "[" + i.getValueReceived() + "]");
          return false;
        }
      }
    }
    return true;
  }

  public static void main(String[] args) throws IOException {
    String directoryName = "./" + DIR_SEND;
    File directory = new File(directoryName);
    if (!directory.exists()) {
      directory.mkdir();
    }
    Path dir = Paths.get(directoryName);
    new FileWatchService(dir).processEvents();
  }
}
