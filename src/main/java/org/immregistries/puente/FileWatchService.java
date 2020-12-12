package org.immregistries.puente;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.immregistries.mqe.hl7util.SeverityLevel;
import org.immregistries.mqe.validator.detection.ValidationReport;
import org.immregistries.mqe.validator.engine.MessageValidator;
import org.immregistries.mqe.validator.engine.ValidationRuleResult;
import org.immregistries.mqe.vxu.MqeMessageReceived;
import org.immregistries.mqe.vxu.MqeAddress;
import org.immregistries.mqe.vxu.MqePatient;
import org.immregistries.mqe.vxu.MqeVaccination;
import org.immregistries.mqe.vxu.MqeMessageHeader;

public class FileWatchService {
  private final WatchService watcher;
  private final Map<WatchKey, Path> keys;
  private MessageValidator validator = MessageValidator.INSTANCE;

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
    Iterable<CSVRecord> records =
        CSVFormat.EXCEL.withFirstRecordAsHeader().parse(new FileReader(file));
    for (CSVRecord record : records) {
      System.out.println(record);

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
      SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmssZ");
      Date date = new Date(System.currentTimeMillis());
      header.setMessageDateString(formatter.format(date));
      header.setMessageDate(date);
      MqePatient patient = mmr.getPatient();
      MqeAddress address = patient.getPatientAddress();
      List<MqeVaccination> vaccinations = mmr.getVaccinations();
      MqeVaccination vaccination = new MqeVaccination();
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
      reportResults(list);
    }
  }

  private void reportResults(List<ValidationRuleResult> list) {
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
        }
      }
    }
  }


  public static void main(String[] args) throws IOException {
    Path dir = Paths.get(".");
    new FileWatchService(dir).processEvents();
  }
}
