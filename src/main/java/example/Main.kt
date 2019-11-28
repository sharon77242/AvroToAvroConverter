package example

import example.generated.*
import org.apache.avro.specific.SpecificRecordBase
import java.util.*
import java.util.logging.Logger

object Main {
    private val LOGGER = Logger.getLogger(Main::class.java.name)
    private fun generateInputRecordExample(): SpecificRecordBase {
        val child1 = Child.newBuilder().setName("noob").build()
        val child2 = Child.newBuilder().setName("noob2").build()
        val children: MutableList<Child> = ArrayList()
        children.add(child1)
        children.add(child2)
        val additional: MutableMap<String, String> = HashMap()
        additional["shit1"] = "shit2"
        additional["shit3"] = "shit4"
        return BdPerson.newBuilder()
                .setIdentification(
                        Identification.newBuilder()
                                .setId(2)
                                //.setUsername("sharone")
                                .build())
                .setUsername("mrscarter")
                .setFirstName("Beyonce")
                .setLastName("Knowles-Carter")
                .setBirthdate("1981-09-04")
                .setPhoneNumber("555555555")
                .setMiddleName("kaka")
                .setSex("Man")
                .setCards(Cards.CLUBS)
                .setChildren(children)
                .setAdditional(additional)
                .build()
    }

    private fun generateOutputRecordExample(): SpecificRecordBase {
        return BdPersonOut.newBuilder()
                .setIdentificationout(
                        IdentificationOut.newBuilder()
                                .setIdout(3)
                                .setUsernameout("sharone1")
                                .build())
                .setHeight(1.84)
                .setCardsout(CardsOut.DIAMONDS)
                .setChildrenout(ArrayList())
                .setAdditionalout(HashMap())
                .build()
    }

    // TODO: this configuration should read from configfile or json
    private fun generateExampleConfig(): Map<String, FieldConfiguration> {
        val fieldConfigurations: MutableMap<String, FieldConfiguration> = hashMapOf()
        var inputPath: Queue<String>
        var outputPath: Queue<String>

        // Try Copy a initialized field on input record
        inputPath = LinkedList();
        inputPath.add("identification");
        inputPath.add("id");
        outputPath = LinkedList();
        outputPath.add("identificationout");
        outputPath.add("idout");
        fieldConfigurations["idout"] = FieldConfiguration(inputPath, outputPath);

        // Try Copy a non initialized field on input record
        inputPath = LinkedList()
        inputPath.add("identification")
        inputPath.add("username")
        outputPath = LinkedList()
        outputPath.add("identificationout")
        outputPath.add("usernameout")
        fieldConfigurations["usernameout"] = FieldConfiguration(inputPath, outputPath)

        inputPath = LinkedList()
        inputPath.add("cards")
        outputPath = LinkedList()
        outputPath.add("cardsout")
        fieldConfigurations["cardsout"] = FieldConfiguration(inputPath, outputPath)

        // Try Copy a non initialized field on input record
        inputPath = LinkedList()
        inputPath.add("children")
        outputPath = LinkedList()
        outputPath.add("childrenout")
        fieldConfigurations.put("childrenout", FieldConfiguration(inputPath, outputPath))

        inputPath = LinkedList()
        inputPath.add("additional")
        outputPath = LinkedList()
        outputPath.add("additionalout")
        fieldConfigurations["additionalout"] = FieldConfiguration(inputPath, outputPath)
        return fieldConfigurations
    }

    private fun exampleConvertNewRecord() {
        val fieldConfigurations = generateExampleConfig()
        val inputRecord = generateInputRecordExample()
        val outSchemaExample = BdPersonOut.`SCHEMA$`
        val avroToAvroConverter = AvroToAvroConverter(fieldConfigurations, outSchemaExample)
        val outRecord = avroToAvroConverter.convertToNewRecord(inputRecord, outSchemaExample)
        if (outRecord != null) {
            LOGGER.info("After converting out new record is: $outRecord")
            return
        }
        LOGGER.info("converting failed")
    }

    private fun exampleConvertExistingRecord() {
        val fieldConfigurations = generateExampleConfig()
        val inputRecord = generateInputRecordExample()
        val outRecordExample = generateOutputRecordExample()
        val avroToAvroConverter = AvroToAvroConverter(fieldConfigurations, outRecordExample.schema)
        val outRecord = avroToAvroConverter.convertToExistingRecord(inputRecord, outRecordExample)
        if (outRecord != null) {
            LOGGER.info("After converting out existing record is: $outRecord")
            return
        }
        LOGGER.info("converting failed")
    }

    @JvmStatic
    fun main(args: Array<String>) {
        //exampleConvertNewRecord()
        exampleConvertExistingRecord();
    }
}