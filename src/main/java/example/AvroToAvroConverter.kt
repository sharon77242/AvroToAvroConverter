package example

import org.apache.avro.Schema
import org.apache.avro.specific.SpecificData
import org.apache.avro.specific.SpecificRecordBase
import java.util.*
import java.util.function.Consumer
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.collections.ArrayList

// TODO: add unit tests
// TODO: add documentation

private const val INPUT_FIELD_ERROR = "Could not find a field named: %s on input schema: %s"
private const val OUTPUT_FIELD_ERROR = "Could not find a field named: %s on output schema: %s"

/**
 * This class is a converter from one avro to another with a configuration file
 * it also validates requires fields are provided in configuration,
 * the convert has to have same field values on input and output schema, there are validation for that
 */
class AvroToAvroConverter(private val fieldConfigurations: Map<String, FieldConfiguration>, outputSchema: Schema) {
    private val logger: Logger = Logger.getLogger(AvroToAvroConverter::class.java.name)
    private val requiredFieldsOnOutSchema: MutableList<Queue<String>> = ArrayList()

    init {
        require(fieldConfigurations.isNotEmpty()) { "field configuration must be not empty" }
        checkRequiredFieldsProvidedInConfig(outputSchema)
        logger.info("Required field paths are: ${this.requiredFieldsOnOutSchema}")
    }

    private fun getInputRecordValue(inputRecord: SpecificRecordBase, inputPath: Queue<String>): Any? {
        var currentRecord = inputRecord
        var schema = inputRecord.schema
        while (inputPath.size > 1) {
            val field = getNextField(inputPath, schema, INPUT_FIELD_ERROR)
            schema = field.schema()
            currentRecord = getInnerRecord(currentRecord, field)
        }
        val field = getNextField(inputPath, schema, INPUT_FIELD_ERROR)
        return currentRecord[field.pos()]
    }

    private fun putToOutRecord(outputRecord: SpecificRecordBase,
                               outputPath: Queue<String>,
                               value: Any?,
                               fieldOutName: String) {
        var valueCopy = value
        var currentRecord = outputRecord
        var schema = outputRecord.schema
        var field: Schema.Field
        while (outputPath.size > 1) {
            field = getNextField(outputPath, schema, OUTPUT_FIELD_ERROR)
            schema = field.schema()
            initNewInstanceIfNeeded(currentRecord, schema, field)
            currentRecord = getInnerRecord(currentRecord, field)
        }
        if (valueCopy == null) {
            currentRecord.put(fieldOutName, null)
            return
        }

        field = getNextField(outputPath, schema, OUTPUT_FIELD_ERROR)
        schema = field.schema()
        valueCopy = getValueForEnum(valueCopy, schema)
        tryPutingRecord(valueCopy, fieldOutName, currentRecord, schema)
    }

    private fun tryPutingRecord(value: Any, fieldOutName: String, currentRecord: SpecificRecordBase, schema: Schema) {
        try {
            currentRecord.put(fieldOutName, value)
        } catch (e: ClassCastException) {
            throw RuntimeException(String.format(
                    "Could not cast field %s because of different type on input schema %s than expected %s on output schema",
                    fieldOutName, value.javaClass, schema.fullName))
        }
    }

    private fun getValueForEnum(value: Any, schema: Schema): Any {
        if (schema.type == Schema.Type.ENUM) {
            return SpecificData.get().createEnum(value.toString(), schema)
        }
        return value
    }

    private fun getNextField(path: Queue<String>, schema: Schema, errorMessage: String): Schema.Field {
        val currentInputPath: String = path.remove()
        return tryGettingFieldFromSchema(schema, currentInputPath, errorMessage)
    }

    private fun getInnerRecord(currentRecord: SpecificRecordBase, field: Schema.Field): SpecificRecordBase {
        return currentRecord[field.pos()] as SpecificRecordBase
    }

    private fun tryGettingFieldFromSchema(schema: Schema, currentPath: String, errorMessage: String): Schema.Field {
        var schemaCopy = schema
        if (schemaCopy.isUnion) {
            schemaCopy = getUnionSchema(schemaCopy)
        }
        val field = schemaCopy.getField(currentPath)
        Objects.requireNonNull(field, String.format(errorMessage, currentPath, schemaCopy.fullName))
        return field
    }

    private fun getUnionSchema(schema: Schema): Schema {
        return schema.types
                .stream()
                .filter { schema1: Schema -> schema1.type != Schema.Type.NULL }
                .reduce { a: Schema, b: Schema -> throw IllegalStateException("Multiple elements: $a, $b") }
                .get()
    }

    /**
     * This function checks if currentRecord is initialized in hierarchy, if not it initializes it
     *
     * @param currentRecord record to check
     * @param schema        schema to create into the current record
     * @param field         field to create into current record
     */
    private fun initNewInstanceIfNeeded(currentRecord: SpecificRecordBase, schema: Schema, field: Schema.Field) {
        if (currentRecord[field.pos()] == null) {
            val newInstance = createNewInstance(schema)
            Objects.requireNonNull(newInstance, String.format("Could not create for field with schema %s", schema))
            currentRecord.put(field.name(), newInstance)
        }
    }

    private fun createNewInstance(schema: Schema): Any {
        var schemaCopy: Schema = schema
        if (schema.isUnion) {
            schemaCopy = getUnionSchema(schema)
        }
        return SpecificData.newInstance(Class.forName(schemaCopy.fullName), schema)
    }

    private fun primitiveField(field: Schema.Field): Boolean {
        val type = field.schema().type
        return type != Schema.Type.RECORD && type != Schema.Type.UNION
    }

    private fun outConfContainsField(field: String): Boolean {
        return fieldConfigurations.filterValues { configuration -> configuration.checkExistenceInOutPath(field) }.isNotEmpty()
    }

    private fun checkRequiredFieldsProvidedInConfig(outputSchema: Schema) {
        outputSchema.fields
                .forEach(Consumer { field: Schema.Field ->
                    val fieldName = field.name()
                    if (primitiveField(field)) {
                        val requiredField = !field.hasDefaultValue()
                        if (requiredField) {
                            throwNonContainedField(fieldName)
                            fieldConfigurations[fieldName]?.outputPath?.let { requiredFieldsOnOutSchema.add(it) }
                        }
                    } else {
                        checkNonPrimitive(field, fieldName)
                    }
                })
    }

    private fun checkNonPrimitive(field: Schema.Field, fieldName: String) {
        var fieldSchema = field.schema()
        if (fieldSchema.isUnion) {
            if (!outConfContainsField(fieldName)) {
                return
            }

            fieldSchema = getUnionSchema(fieldSchema)
        }
        if (fieldSchema.type == Schema.Type.RECORD) {
            checkRequiredFieldsProvidedInConfig(fieldSchema)
        }
    }

    private fun throwNonContainedField(fieldName: String) {
        val hasRequiredField = fieldConfigurations.containsKey(fieldName)
        if (!hasRequiredField) {
            throw RuntimeException(String.format(
                    "Field named %s is a required field in output schema but is not provided in config",
                    fieldName))
        }
    }

    private fun baseConverter(inputRecord: SpecificRecordBase,
                              outputRecord: SpecificRecordBase): SpecificRecordBase {
        fieldConfigurations.forEach { (fieldName, conf: FieldConfiguration) ->
            val inputValue = getInputRecordValue(inputRecord, conf.inputPath)
            if (inputValue == null && requiredFieldsOnOutSchema.contains(conf.outputPath)) {
                throw RuntimeException("Input record did not contain value for a required field: $fieldName")
            }
            putToOutRecord(outputRecord, conf.outputPath, inputValue, fieldName)
        }

        return outputRecord
    }

    /**
     * This function converts with a given configuration from an input record to an out new record
     *
     * @param inputRecord         input record to convert from
     * @param outputSchema        output schema to convert to
     * @return modified and converted output record
     */
    fun convertToNewRecord(
            inputRecord: SpecificRecordBase,
            outputSchema: Schema): SpecificRecordBase? {
        val outputRecord: SpecificRecordBase
        return try {
            outputRecord = createNewInstance(outputSchema) as SpecificRecordBase
            baseConverter(inputRecord, outputRecord)
        } catch (e: Exception) {
            logger.log(Level.SEVERE, "Error occurred: ", e)
            null
        }
    }

    /**
     * This function converts with a given configuration from an input record to an out *existing* output record
     * Caution : it modifies the output record and can overwrite existing fields,
     * but not the parameter reference only by its return value
     *
     * @param inputRecord         input record to convert from
     * @param outputRecord        output record to convert to
     * @return modified and converted output record
     */
    fun convertToExistingRecord(
            inputRecord: SpecificRecordBase,
            outputRecord: SpecificRecordBase): SpecificRecordBase? {
        val outputRecordCopy: SpecificRecordBase
        return try {
            outputRecordCopy = SpecificData.get().deepCopy(outputRecord.schema, outputRecord)
            baseConverter(inputRecord, outputRecordCopy)
        } catch (e: Exception) {
            logger.log(Level.SEVERE, "Error occurred: ", e)
            null
        }
    }
}