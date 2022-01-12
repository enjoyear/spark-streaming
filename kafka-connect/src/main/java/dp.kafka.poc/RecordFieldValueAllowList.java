package dp.kafka.poc;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.predicates.Predicate;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A Kafka Connect predicate that checks whether a record's field value is in the configured allowed list
 */
// Triggering Example:
//   curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d '{
//    "name": "chen-test",
//    "config": {
//      "connector.class": "FileStreamSink",
//      "tasks.max": 1,
//      "file": "/tmp/test.txt",
//      "topics": "mytest",
//      "value.converter.schemas.enable": false,
//      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
//
//      "transforms": "filter",
//      "transforms.filter.type": "org.apache.kafka.connect.transforms.Filter",
//      "transforms.filter.predicate": "fieldAllowedList",
//      "transforms.filter.negate": true,
//
//      "predicates": "fieldAllowedList",
//      "predicates.fieldAllowedList.type": "dp.kafka.poc.RecordFieldValueAllowList",
//      "predicates.fieldAllowedList.fieldName": "eventName",
//      "predicates.fieldAllowedList.allowedList": " e1,Event2, EVENT3 "
//    }
//  }'
//
// Note that multiple predicates are not supported by the filter
// https://github.com/apache/kafka/blob/c182a431d224cb39c0bb43a55199e2d8b4aee1b7/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/PredicatedTransformation.java#L37

public class RecordFieldValueAllowList<R extends ConnectRecord<R>> implements Predicate<R> {
    public static final String OVERVIEW_DOC = "A predicate which checks a record's field value is in configured allowed list";

    private static final String FIELD_NAME_CONFIG = "fieldName";
    private String _fieldName;

    private static final String ALLOW_LIST_SPLIT_CHARACTER = ",";
    private static final String ALLOWED_LIST_CONFIG = "allowedList";
    private Set<String> _allowedList;

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    new ConfigDef.NonEmptyString(), ConfigDef.Importance.MEDIUM,
                    "The name of the record's payload field to verify. This name is case sensitive.")
            .define(ALLOWED_LIST_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    new ConfigDef.NonEmptyString(), ConfigDef.Importance.MEDIUM,
                    "The list of allowed values separated by comma for the field. The values are case in-sensitive.");

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public boolean test(R record) {
        Object payload = record.value();
        if (!(payload instanceof HashMap)) {
            throw new RuntimeException(String.format(
                    "Expect the record(partition %d, offset %s) payload to be a HashMap. Non-JSON types are currently not supported",
                    record.kafkaPartition(),
                    record instanceof SinkRecord ? ((SinkRecord) record).kafkaOffset() : "n/a"));
        }

        HashMap<String, Object> payloadMap = (HashMap<String, Object>) payload;
        Object fieldValue = payloadMap.get(this._fieldName);
        if (!(fieldValue instanceof String)) {
            throw new RuntimeException(String.format(
                    "Expect the field %s of record(partition %d, offset %s) payload to be a String type",
                    this._fieldName,
                    record.kafkaPartition(),
                    record instanceof SinkRecord ? ((SinkRecord) record).kafkaOffset() : "n/a"));
        }

        String stringVal = (String) fieldValue;
        return _allowedList.contains(stringVal);
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        SimpleConfig simpleConfig = new SimpleConfig(config(), configs);
        this._fieldName = simpleConfig.getString(FIELD_NAME_CONFIG);
        this._allowedList = Arrays.stream(simpleConfig.getString(ALLOWED_LIST_CONFIG).split(ALLOW_LIST_SPLIT_CHARACTER))
                .map(x -> x.trim().toLowerCase()).collect(Collectors.toUnmodifiableSet());
    }

    public static void main(String[] args) {
    }
}
