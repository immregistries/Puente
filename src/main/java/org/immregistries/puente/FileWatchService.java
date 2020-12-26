package org.immregistries.puente;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
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
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.RandomStringUtils;
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
  private static final String PARAM_RECIPIENT_RACE_6 = "Recipient race 6";
  private static final String PARAM_RECIPIENT_RACE_5 = "Recipient race 5";
  private static final String PARAM_RECIPIENT_RACE_4 = "Recipient race 4";
  private static final String PARAM_RECIPIENT_RACE_3 = "Recipient race 3";
  private static final String PARAM_RECIPIENT_RACE_2 = "Recipient race 2";
  private static final String PARAM_RECIPIENT_ETHNICITY = "Recipient ethnicity";
  private static final String PARAM_RECIPIENT_RACE_1 = "Recipient race 1";
  private static final String PARAM_ADMINISTERED_AT_LOCATION = "Administered at location";
  private static final String PARAM_RESPONSIBLE_ORGANIZATION = "Responsible organization";
  private static final String PARAM_VACCINE_ROUTE_OF_ADMINISTRATION =
      "Vaccine route of administration";
  private static final String PARAM_VACCINE_ADMINISTERING_SITE = "Vaccine administering site";
  private static final String PARAM_VACCINE_EXPIRATION_DATE = "Vaccine expiration date";
  private static final String PARAM_LOT_NUMBER = "Lot number";
  private static final String PARAM_MVX2 = "MVX";
  private static final String PARAM_NDC2 = "NDC";
  private static final String PARAM_CVX2 = "CVX";
  private static final String PARAM_ADMINISTRATION_DATE = "Administration date";
  private static final String PARAM_RECIPIENT_ADDRESS_ZIP_CODE = "Recipient address: zip code";
  private static final String PARAM_RECIPIENT_ADDRESS_STATE = "Recipient address: state";
  private static final String PARAM_RECIPIENT_ADDRESS_COUNTY = "Recipient address: county";
  private static final String PARAM_RECIPIENT_ADDRESS_CITY = "Recipient address: city";
  private static final String PARAM_RECIPIENT_ADDRESS_STREET_2 = "Recipient address: street 2";
  private static final String PARAM_RECIPIENT_ADDRESS_STREET = "Recipient address: street";
  private static final String PARAM_RECIPIENT_SEX = "Recipient sex";
  private static final String PARAM_RECIPIENT_DATE_OF_BIRTH = "Recipient date of birth";
  private static final String PARAM_RECIPIENT_NAME_LAST = "Recipient name: last";
  private static final String PARAM_RECIPIENT_NAME_MIDDLE = "Recipient name: middle";
  private static final String PARAM_RECIPIENT_NAME_FIRST = "Recipient name: first";
  private static final String PARAM_RECIPIENT_ID = "Recipient ID";
  private static final String PARAM_VACCINATION_EVENT_ID = "Vaccination event ID";
  private static final String PARAM_VACCINATION_REFUSAL = "Vaccination refusal";

  private static final String[] REQUIRED_HEADERS =
      {PARAM_RECIPIENT_ID, PARAM_RECIPIENT_NAME_FIRST, PARAM_RECIPIENT_NAME_LAST,
          PARAM_RECIPIENT_DATE_OF_BIRTH, PARAM_RECIPIENT_SEX, PARAM_ADMINISTRATION_DATE};

  private final WatchService watcher;
  private final Map<WatchKey, Path> keys;
  private static MessageValidator validator = MessageValidator.INSTANCE;

  private static final String DIR_DATA = "data";
  private static final String DIR_DATA_ERROR = "error";
  private static final String DIR_DATA_READY = "ready";
  private static final String DIR_REQUEST = "request";

  private static final String vxuTemplate =
      "MSH|^~\\&|||||${messageHeaderDate}||VXU^V04^VXU_V04|J69O9.9l|P|2.5.1|||ER|AL|||||Z22^CDCPHINVS|\n"
          + "PID|1||${recipientId}^^^AIRA-TEST^MR||${lastName}^${firstName}^${middleName}^^^^L||${birthDate}|${sex}||${pid10}|${street}^${street2}^${city}^${state}^${zipCode}^USA^P||^PRN^PH^^^734^9473420|||||||||${ethnicity}|\n"
          + "PD1|||||||||||02^Reminder/Recall - any method^HL70215|||||A|20201214|20201214|\n"
          + "ORC|RE||${vaccinationEventId}^AIRA|\n"
          + "RXA|0|1|${administrationDate}||${administeredCode}|999|||01^Historical^NIP001||||||${lotNumber}||${mvx}|||CP|A|\n";

  private static final String vxuRefusalTemplate =
      "MSH|^~\\&|||||${messageHeaderDate}||VXU^V04^VXU_V04|J69O9.9l|P|2.5.1|||ER|AL|||||Z22^CDCPHINVS|\n"
          + "PID|1||${recipientId}^^^AIRA-TEST^MR||${lastName}^${firstName}^${middleName}^^^^L||${birthDate}|${sex}||${pid10}|${street}^${street2}^${city}^${state}^${zipCode}^USA^P||^PRN^PH^^^734^9473420|||||||||${ethnicity}|\n"
          + "PD1|||||||||||02^Reminder/Recall - any method^HL70215|||||A|20201214|20201214|\n"
          + "ORC|RE||${vaccinationEventId}^AIRA|\n"
          + "RXA|0|1|${administrationDate}||${administeredCode}|999|||||||||${lotNumber}||${mvx}|00^Parental decision^NIP002||RE|A|\n";

  FileWatchService(Path dir) throws IOException {
    this.watcher = FileSystems.getDefault().newWatchService();
    this.keys = new HashMap<WatchKey, Path>();

    registerDirectory(dir);
  }

  private void registerDirectory(Path dir) throws IOException {
    WatchKey key = dir.register(watcher, ENTRY_CREATE);
    keys.put(key, dir);
  }

  void processEvents() throws IOException {
    for (;;) {
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

  static void evaluateFile(File file) throws IOException {
    if (file.isDirectory()) {
      return;
    }
    System.out.println("Reading " + file.getName());
    File errorFile = null;
    File readyFile = null;
    File hl7File = null;
    Iterable<CSVRecord> records = new ArrayList<CSVRecord>();
    List<String> headers = new ArrayList<String>();
    FileReader fileReader = new FileReader(file);
    try {
      try {
        records = CSVFormat.DEFAULT.withFirstRecordAsHeader().withTrim().parse(fileReader);
        BufferedReader br = new BufferedReader(new FileReader(file));
        CSVParser parser = CSVParser.parse(br, CSVFormat.DEFAULT.withFirstRecordAsHeader().withTrim());
        headers = parser.getHeaderNames();
        parser.close();
      } catch (Exception e) {
        System.out.println("Couldn't parse file.");
        e.printStackTrace();
      }

      boolean okayToRead = true;
      for (String requiredHeader : REQUIRED_HEADERS) {
        if (!headers.contains(requiredHeader)) {
          System.err.println("  + Missing required column: " + requiredHeader);
          okayToRead = false;
        }
      }

      if (!okayToRead) {
        return;
      }

      int countTotal = 0;
      int countError = 0;
      int countOkay = 0;

      for (CSVRecord record : records) {
        // System.out.println(record);
        countTotal++;

        String refusal = defaultedGet(record, PARAM_VACCINATION_REFUSAL);
        String vaccinationEventId = defaultedGet(record, PARAM_VACCINATION_EVENT_ID);
        String recipientId = defaultedGet(record, PARAM_RECIPIENT_ID);
        String firstName = defaultedGet(record, PARAM_RECIPIENT_NAME_FIRST);
        String middleName = defaultedGet(record, PARAM_RECIPIENT_NAME_MIDDLE);
        String lastName = defaultedGet(record, PARAM_RECIPIENT_NAME_LAST);
        String birthDate = defaultedGet(record, PARAM_RECIPIENT_DATE_OF_BIRTH);
        String sex = defaultedGet(record, PARAM_RECIPIENT_SEX);
        String street = defaultedGet(record, PARAM_RECIPIENT_ADDRESS_STREET);
        String street2 = defaultedGet(record, PARAM_RECIPIENT_ADDRESS_STREET_2);
        String city = defaultedGet(record, PARAM_RECIPIENT_ADDRESS_CITY);
        String county = defaultedGet(record, PARAM_RECIPIENT_ADDRESS_COUNTY);
        String state = defaultedGet(record, PARAM_RECIPIENT_ADDRESS_STATE);
        String zipCode = defaultedGet(record, PARAM_RECIPIENT_ADDRESS_ZIP_CODE);
        String administrationDate = defaultedGet(record, PARAM_ADMINISTRATION_DATE);
        String cvx = defaultedGet(record, PARAM_CVX2);
        String ndc = defaultedGet(record, PARAM_NDC2);
        String mvx = defaultedGet(record, PARAM_MVX2);
        String lotNumber = defaultedGet(record, PARAM_LOT_NUMBER);
        String vaccineExpDate = defaultedGet(record, PARAM_VACCINE_EXPIRATION_DATE);
        String vaccineAdmSite = defaultedGet(record, PARAM_VACCINE_ADMINISTERING_SITE);
        String vaccineRoute = defaultedGet(record, PARAM_VACCINE_ROUTE_OF_ADMINISTRATION);
        String responsibleOrg = defaultedGet(record, PARAM_RESPONSIBLE_ORGANIZATION);
        String admAtLoc = defaultedGet(record, PARAM_ADMINISTERED_AT_LOCATION);
        String race1 = defaultedGet(record, PARAM_RECIPIENT_RACE_1);
        String ethnicity = defaultedGet(record, PARAM_RECIPIENT_ETHNICITY);

        ArrayList<String> races = new ArrayList<>();
        races.add(defaultedGet(record, PARAM_RECIPIENT_RACE_1));
        races.add(defaultedGet(record, PARAM_RECIPIENT_RACE_2));
        races.add(defaultedGet(record, PARAM_RECIPIENT_RACE_3));
        races.add(defaultedGet(record, PARAM_RECIPIENT_RACE_4));
        races.add(defaultedGet(record, PARAM_RECIPIENT_RACE_5));
        races.add(defaultedGet(record, PARAM_RECIPIENT_RACE_6));

        String pid10 = "";
        for (String race : races) {
          if (!"".equals(race)) {
            if (!"".equals(pid10)) {
              pid10 += "^";
            }
            pid10 += race + "^^CDCREC";
          }
        }

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
        vaccination.setLotNumber(lotNumber);
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
        patient.setRace(race1);
        patient.setEthnicity(ethnicity);

        List<ValidationRuleResult> list = validator.validateMessage(mmr);
        String errorStr = reportResults(list);
        if (errorStr != null) {
          errorFile = writeErrorFile(errorStr, record, file.getName(), errorFile, headers);
          countError++;
        } else {
          countOkay++;
          Map<String, String> valuesMap = new HashMap<>();
          valuesMap.put("messageHeaderDate", header.getMessageDateString());
          valuesMap.put("recipientId", recipientId);
          valuesMap.put("lastName", lastName);
          valuesMap.put("firstName", firstName);
          valuesMap.put("middleName", middleName);
          valuesMap.put("birthDate", birthDate);
          valuesMap.put("sex", sex);
          valuesMap.put("street", street);
          valuesMap.put("street2", street2);
          valuesMap.put("city", city);
          valuesMap.put("state", state);
          valuesMap.put("zipCode", zipCode);
          valuesMap.put("administrationDate", administrationDate);
          valuesMap.put("pid10", pid10);
          valuesMap.put("lotNumber", lotNumber);
          if (!"".equals(ethnicity)) {
            ethnicity += "^^CDCREC";
          }
          valuesMap.put("ethnicity", ethnicity);

          if ("".equals(vaccinationEventId)) {
            vaccinationEventId = RandomStringUtils.randomAlphanumeric(10);
          }
          valuesMap.put("vaccinationEventId", vaccinationEventId);

          if (!"".equals(mvx)) {
            mvx += "^^MVX";
          }
          valuesMap.put("mvx", mvx);

          String administeredCode = "";
          if (!"".equals(cvx)) {
            administeredCode = cvx + "^^CVX";
          } else if (!"".equals(ndc)) {
            administeredCode = ndc + "^^NDC";
          }
          valuesMap.put("administeredCode", administeredCode);
          StringSubstitutor sub = new StringSubstitutor(valuesMap);

          String resolvedString = "";
          if(vaccineRefused(refusal.toUpperCase())){
            resolvedString = sub.replace(vxuRefusalTemplate);
          }else{
            resolvedString = sub.replace(vxuTemplate);
          }

          if (!"".equals(vaccineRoute)) {
            String rxr = "RXR|" + vaccineRoute + "^^NCIT|";
            if (!"".equals(vaccineAdmSite)) {
              rxr += vaccineAdmSite + "^^HL70163";
            }
            resolvedString += rxr + "\n";
          }
          System.out.println(resolvedString);
          hl7File = writeFile(file.getName(), resolvedString + "\n", hl7File);
          readyFile = writeReadyFile(record, file.getName(), readyFile, headers);
        }
      }
      System.out.println("  + Total records: " + countTotal);
      if (countError > 0) {
        System.out.println("  + Errors found: " + countError);
      }
      if (countOkay > 0) {
        System.out.println("  + HL7 messages created: " + countOkay);
      }
    } finally {
      fileReader.close();
    }
    if(file.delete()){
      System.out.println("  + Original file deleted");
    }else{
      System.out.println("  + File deletion failed");
    }
  }

  static String defaultedGet(CSVRecord record, String name) {
    String retStr = "";
    if (record.isMapped(name)) {
      retStr = record.get(name);
    }
    if (retStr == null) {
      retStr = "";
    }
    return retStr.trim();
  }

  static boolean vaccineRefused(String refusal){
      boolean retVal = false;
      if(!"".equals(refusal) && !"NO".equals(refusal)) {
          retVal = true;
      }
      return retVal;
  }

  static File writeReadyFile(CSVRecord record, String name, File file, List<String> headers)
      throws IOException {
    String directoryName = "./" + DIR_DATA + "/" + DIR_DATA_READY;

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

      FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
      BufferedWriter bw = new BufferedWriter(fw);
      String recordString = "";
      for (String s : headers) {
        recordString += s + ",";
      }
      bw.write(recordString + "\n");
      bw.close();
    }

    FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
    BufferedWriter bw = new BufferedWriter(fw);
    String recordString = "";
    Iterator<String> itr = record.iterator();
    while (itr.hasNext()) {
      recordString += itr.next();
      if (itr.hasNext()) {
        recordString += ",";
      }
    }
    bw.write(recordString + "\n");
    bw.close();

    return file;
  }

  static File writeErrorFile(String errorString, CSVRecord record, String name, File file,
      List<String> headers) throws IOException {
    String directoryName = "./" + DIR_DATA + "/" + DIR_DATA_ERROR;

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

      FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
      BufferedWriter bw = new BufferedWriter(fw);
      String recordString = "Error";
      for (String s : headers) {
        recordString += "," + s;
      }
      bw.write(recordString + "\n");
      bw.close();
    }

    FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
    BufferedWriter bw = new BufferedWriter(fw);
    String recordString = "" + errorString + ",";
    Iterator<String> itr = record.iterator();
    while (itr.hasNext()) {
      recordString += itr.next();
      if (itr.hasNext()) {
        recordString += ",";
      }
    }
    bw.write(recordString + "\n");
    bw.close();

    return file;
  }

  static File writeFile(String name, String value, File file) throws IOException {
    String directoryName = "./" + DIR_REQUEST;
    System.out.println("Writing file");

    File directory = new File(directoryName);
    if (!directory.exists()) {
      directory.mkdir();
    }

    if (file == null) {
      SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
      Date date = new Date(System.currentTimeMillis());
      String dateStr = formatter.format(date);
      String fileName = name.split("\\.")[0] + "-" + dateStr + ".hl7";
      file = new File(directoryName + "/" + fileName);
    }

    FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
    BufferedWriter bw = new BufferedWriter(fw);
    bw.write(value);
    bw.close();

    return file;
  }

  private static String reportResults(List<ValidationRuleResult> list) {
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
          // System.out.println(s + ": " + i.getDetection() + "[" + i.getValueReceived() + "]");
          return i.getDetection().toString();
        }
      }
    }
    return null;
  }

  public static void main(String[] args) throws IOException {
    String directoryName = "./" + DIR_DATA;
    File directory = new File(directoryName);
    if (!directory.exists()) {
      directory.mkdir();
    }
    Path dir = Paths.get(directoryName);
    try {
      DirectoryStream<Path> directoryStream = Files.newDirectoryStream(dir);
      for (Path entry : directoryStream) {
        evaluateFile(entry.toFile());
      }
    } catch (DirectoryIteratorException ex) {
      System.out.println("Exception checking for existing send files");
    }
    new FileWatchService(dir).processEvents();
  }
}
