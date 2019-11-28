package example

import java.util.*

class FieldConfiguration(val inputPath: Queue<String>, val outputPath: Queue<String>) {

    public fun checkExistenceInOutPath(fieldInPath: String): Boolean {
        return this.outputPath.contains(fieldInPath);
    }
}