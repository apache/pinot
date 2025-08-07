import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.plugin.inputformat.parquet.ParquetUtils;
import org.apache.pinot.tools.AbstractBaseCommand;
import org.apache.pinot.tools.Command;
import picocli.CommandLine;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;


@CommandLine.Command(name = "SchemaFromAvroParquet", mixinStandardHelpOptions = true, description = "Generates a Pinot schema from an Avro or Parquet schema.")
public class SchemaFromAvroParquetCommand extends AbstractBaseCommand implements Command {

    @CommandLine.Option(names = {"-input"}, required = true, description = "Path to the input Avro or Parquet file.")
    private String _inputFile;

    @CommandLine.Option(names = {"-output"}, required = true, description = "Path to the output Pinot schema file.")
    private String _outputFile;

    @CommandLine.Option(names = {"-format"}, required = true, description = "Input format (AVRO or PARQUET).")
    private String _format;

    @Override
    public boolean execute() throws Exception {
        if ("AVRO".equalsIgnoreCase(_format)) {
            return generateSchemaFromAvro();
        } else if ("PARQUET".equalsIgnoreCase(_format)) {
            return generateSchemaFromParquet();
        } else {
            System.err.println("Unsupported format: " + _format);
            return false;
        }
    }

    private boolean generateSchemaFromAvro() throws IOException {
        try (FileInputStream inputStream = new FileInputStream(_inputFile)) {
            org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(inputStream);
            Schema pinotSchema = org.apache.pinot.plugin.inputformat.avro.AvroSchemaUtil.createPinotSchema(avroSchema);
            pinotSchema.toPrettyJsonFile(new File(_outputFile));
            System.out.println("Successfully converted Avro schema to Pinot schema at " + _outputFile);
            return true;
        } catch (IOException e) {
            System.err.println("Error reading or parsing Avro file: " + e.getMessage());
            return false;
        }
    }

    private boolean generateSchemaFromParquet() throws IOException {
        try {
            org.apache.avro.Schema avroSchema = ParquetUtils.getParquetAvroSchema(new Path(_inputFile));
            Schema pinotSchema = org.apache.pinot.plugin.inputformat.avro.AvroSchemaUtil.createPinotSchema(avroSchema);
            pinotSchema.toPrettyJsonFile(new File(_outputFile));
            System.out.println("Successfully converted Parquet schema to Pinot schema at " + _outputFile);
            return true;
        } catch (IOException e) {
            System.err.println("Error reading or parsing Parquet file: " + e.getMessage());
            return false;
        }
    }

    @Override
    public String description() {
        return "Converts an Avro or Parquet schema to a Pinot schema.";
    }

    @Override
    public String getName() {
        return "SchemaFromAvroParquet";
    }

    @Override
    public void printUsage() {
        new CommandLine(this).usage(System.out);
    }
}

