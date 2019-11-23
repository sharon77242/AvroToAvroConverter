package example;

import java.util.Queue;

public class FieldConfiguration {
    private String outFieldName;
    private Queue<String> inputPath;
    private Queue<String> outputPath;

    public FieldConfiguration(String outFieldName, Queue<String> inputPath, Queue<String> outputPath) {
        this.outFieldName = outFieldName;
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    public String getOutFieldName() {
        return outFieldName;
    }

    public Queue<String> getInputPath() {
        return inputPath;
    }

    public Queue<String> getOutputPath() {
        return outputPath;
    }
}
