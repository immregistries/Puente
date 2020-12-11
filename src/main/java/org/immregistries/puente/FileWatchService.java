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
import org.immregistries.mqe.vxu.MqePatient;

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
      String firstName = record.get("Recipient name: first");
      String middleName = record.get("Recipient name: middle");
      String lastName = record.get("Recipient name: last");
      String birthDate = record.get("Recipient date of birth");
      String sex = record.get("Recipient sex");
      MqeMessageReceived mmr = new MqeMessageReceived();
      MqePatient patient = mmr.getPatient();
      patient.setNameFirst(firstName);
      patient.setNameMiddle(middleName);
      patient.setNameLast(lastName);
      patient.setBirthDateString(birthDate);
      patient.setSexCode(sex);
      System.out.println(patient);
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
