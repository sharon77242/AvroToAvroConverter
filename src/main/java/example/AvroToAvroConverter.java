package example;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecordBase;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

// TODO: add unit tests
// TODO: add documentation
// TODO: check this works for arrays and enums!!
public class AvroToAvroConverter {
    private final static Logger LOGGER = Logger.getLogger(AvroToAvroConverter.class.getName());
    private final static String INPUT_FIELD_ERROR = "Could not find a field named: %s on input schema";
    private final static String OUTPUT_FIELD_ERROR = "Could not find a field named: %s on output schema";
    private static List<String> requiredFieldsOnOutSchema = new ArrayList<>();

    private static Object getInputRecordValue(SpecificRecordBase inputRecord, Queue<String> inputPath) {
        SpecificRecordBase currentRecord = inputRecord;
        Schema schema = inputRecord.getSchema();
        String currentInputPath;

        while (inputPath.size() > 1) {
            currentInputPath = inputPath.remove();

            Schema.Field field = tryGettingFieldFromSchema(schema, currentInputPath, INPUT_FIELD_ERROR);

            schema = field.schema();

            currentRecord = getInnerRecord(currentRecord, field);
        }

        currentInputPath = inputPath.remove();

        Schema.Field field = tryGettingFieldFromSchema(schema, currentInputPath, INPUT_FIELD_ERROR);

        return currentRecord.get(field.pos());
    }

    private static void putToOutRecord(SpecificRecordBase outputRecord,
                                       Queue<String> outputPath,
                                       Object value,
                                       String fieldOutName)
            throws ClassNotFoundException {
        SpecificRecordBase currentRecord = outputRecord;
        Schema schema = outputRecord.getSchema();
        Schema.Field field = null;

        while (outputPath.size() > 1) {
            String currentOutputPath = outputPath.remove();
            field = tryGettingFieldFromSchema(schema, currentOutputPath, OUTPUT_FIELD_ERROR);

            schema = field.schema();

            initNewInstanceIfNeeded(currentRecord, schema, field);

            currentRecord = getInnerRecord(currentRecord, field);
        }

        if (field != null) {
            currentRecord.put(fieldOutName, value);
        }
    }

    private static SpecificRecordBase getInnerRecord(SpecificRecordBase currentRecord, Schema.Field field) {
        return (SpecificRecordBase) currentRecord.get(field.pos());
    }

    private static Schema.Field tryGettingFieldFromSchema(Schema schema, String currentPath, String errorMessage) {
        Schema.Field field = schema.getField(currentPath);

        Objects.requireNonNull(field, String.format(errorMessage, currentPath));
        return field;
    }

    /**
     * This function checks if currentRecord is initialized in hierarchy if not initialize it
     *
     * @param currentRecord record to check
     * @param schema        schema to create into the current record
     * @param field         field to create into current record
     * @throws ClassNotFoundException thrown when try to init an invalid field type
     */
    private static void initNewInstanceIfNeeded(SpecificRecordBase currentRecord, Schema schema, Schema.Field field) throws ClassNotFoundException {
        if (getInnerRecord(currentRecord, field) == null) {
            Object newInstance = createNewInstance(schema);

            Objects.requireNonNull(newInstance, String.format("Could not create for field with schema %s", schema));

            currentRecord.put(field.name(), newInstance);
        }
    }

    private static Object createNewInstance(Schema schema) throws ClassNotFoundException {
        return SpecificData.newInstance(Class.forName(schema.getFullName()), schema);
    }

    private static void checkForValidParams(
            List<FieldConfiguration> fieldConfigurations,
            SpecificRecordBase inputRecord,
            SpecificRecordBase outputRecord) {
        Objects.requireNonNull(fieldConfigurations, "field Configurations must contain a valid value");

        Objects.requireNonNull(inputRecord, "input Record must contain a valid value");

        Objects.requireNonNull(outputRecord, "output Record must contain a valid value");
    }

    private static boolean primitiveField(Schema.Field field) {
        Schema.Type type = field.schema().getType();
        return type == Schema.Type.BOOLEAN ||
                type == Schema.Type.BYTES ||
                type == Schema.Type.DOUBLE ||
                type == Schema.Type.ENUM ||
                type == Schema.Type.FIXED ||
                type == Schema.Type.FLOAT ||
                type == Schema.Type.INT ||
                type == Schema.Type.LONG ||
                type == Schema.Type.STRING ||
                type == Schema.Type.UNION;
    }

    private static void saveRequiredFields(Schema outputSchema) {
        outputSchema.getFields()
                .forEach((Schema.Field field) -> {
                    if (primitiveField(field)) {
                        boolean requiredField = !field.hasDefaultValue();

                        if (requiredField) {
                            requiredFieldsOnOutSchema.add(field.name());
                        }
                    } else {
                        saveRequiredFields(field.schema());
                    }
                });
    }

    private static void checkRequiredFieldsProvidedInConfig(List<FieldConfiguration> fieldConfigurations) {
        requiredFieldsOnOutSchema.forEach((String fieldName) -> {
            boolean hasRequiredField = fieldConfigurations.stream()
                    .anyMatch((FieldConfiguration fieldConfiguration) ->
                            fieldConfiguration.getOutFieldName().equals(fieldName));

            if (!hasRequiredField) {
                throw new RuntimeException(
                        String.format("Field named %s is a required field in output schema but is not provided in config",
                                fieldName));
            }
        });
    }

    /**
     * This function converts with a given configuration from an input record to an out new record
     *
     * @param fieldConfigurations field configuration to convert
     * @param inputRecord         input record to convert from
     * @param outputSchema        output schema to convert to
     * @return modified and converted output record
     */
    public static Optional<SpecificRecordBase> convertToNewRecord(
            List<FieldConfiguration> fieldConfigurations,
            SpecificRecordBase inputRecord,
            Schema outputSchema) {

        Objects.requireNonNull(outputSchema, "output Schema must contain a valid value");

        saveRequiredFields(outputSchema);

        checkRequiredFieldsProvidedInConfig(fieldConfigurations);

        SpecificRecordBase outputRecord;
        try {
            outputRecord = (SpecificRecordBase) createNewInstance(outputSchema);
        } catch (ClassNotFoundException e) {
            LOGGER.log(Level.SEVERE, "Error occurred: ", e);
            return Optional.empty();
        }

        return convertToExistingRecord(fieldConfigurations, inputRecord, outputRecord);
    }

    /**
     * This function converts with a given configuration from an input record to an out *existing* output record
     * Caution : it modifies the output record and can overwrite existing fields,
     * but not the parameter reference only by its return value
     * <p>
     * //TODO: this function cannot be public for now - there is a problem when using it on existing record
     * default behavior  will allow required fields to be init sometimes to type default value -
     * for example int will be defaulted to 0
     *
     * @param fieldConfigurations field configuration to convert
     * @param inputRecord         input record to convert from
     * @param outputRecord        output record to convert to
     * @return modified and converted output record
     */
    private static Optional<SpecificRecordBase> convertToExistingRecord(
            List<FieldConfiguration> fieldConfigurations,
            SpecificRecordBase inputRecord,
            SpecificRecordBase outputRecord) {
        SpecificRecordBase outputRecordCopy;

        try {
            checkForValidParams(fieldConfigurations, inputRecord, outputRecord);
            saveRequiredFields(outputRecord.getSchema());

            outputRecordCopy = SpecificData.get().deepCopy(outputRecord.getSchema(), outputRecord);

            for (FieldConfiguration conf : fieldConfigurations) {
                String fieldName = conf.getOutFieldName();
                Object inputValue = getInputRecordValue(inputRecord, conf.getInputPath());
                if (inputValue == null && requiredFieldsOnOutSchema.contains(fieldName)) {
                    throw new RuntimeException("Input record did not contain value for a required field: " + fieldName);
                }

                putToOutRecord(outputRecordCopy, conf.getOutputPath(), inputValue, fieldName);
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error occurred: ", e);
            return Optional.empty();
        }

        return Optional.of(outputRecordCopy);
    }
}
