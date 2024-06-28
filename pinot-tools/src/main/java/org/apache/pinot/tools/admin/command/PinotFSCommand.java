package org.apache.pinot.tools.admin.command;

import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.ingestion.batch.spec.PinotFSSpec;
import org.apache.pinot.tools.Command;
import picocli.CommandLine;

import java.io.*;
import org.slf4j.*;
import org.yaml.snakeyaml.Yaml;
import java.net.URI;

@CommandLine.Command(name = "PinotFS")
public class PinotFSCommand extends AbstractBaseAdminCommand implements Command {
    private static final Logger LOGGER = LoggerFactory.getLogger(PinotFSCommand.class);

    @CommandLine.Option(names = {"-fsSpecFile", "-fsSpec"}, required = true,
            description = "Filesystem spec file")
    private String _fsSpecString;

    @CommandLine.Option(names = {"-command"}, description = "The PinotFS command to execute (init, mkdir, delete, move, copy, exists, length, listFiles, copyToLocal, copyFromLocal, isDirectory, lastModified, touch, open)", required = true)
    private String _command;

    @CommandLine.Option(names = {"-uri", "-sourceUri"}, description = "Source URI for the command.")
    private String _sourceUri;

    @CommandLine.Option(names = {"-destUri"}, description = "Destination URI for the command.")
    private String _destUri;

    @CommandLine.Option(names = {"-filePath"}, description = "Local file path for copyToLocal and copyFromLocal commands.")
    private String _filePath;

    @CommandLine.Option(names = {"-recursive"}, description = "Recursive flag for listFiles command.", defaultValue = "false")
    private boolean _recursive;

    @CommandLine.Option(names = {"-overwrite"}, description = "Overwrite flag for move command.", defaultValue = "false")
    private boolean _overwrite;

    @CommandLine.Option(names = {"-forceDelete"}, description = "Force delete flag for delete command.", defaultValue = "false")
    private boolean _forceDelete;

    @CommandLine.Option(names = {"-help", "-h", "--h", "--help"}, description = "Print this message.", help = true)
    private boolean _help;

    @Override
    public boolean getHelp() {
        return _help;
    }

    @Override
    public String getName() {
        return "PinotFS";
    }

    @Override
    public String description() {
        return "Execute commands on PinotFS";
    }

    @Override
    public String toString() {
        return "PinotFSCommand -fsSpec " + _fsSpecString + " -sourceUri " + _sourceUri + " -destUri " + _destUri
                + " -filePath " + _filePath + " -command " + _command + " -forceDelete " + _forceDelete
                + " -overwrite " + _overwrite + " -recursive " + _recursive + " -help " + _help;
    }

    @Override
    public boolean execute() throws Exception {
        if (_help) {
            printUsage();
            return true;
        }

        LOGGER.info("Executing command: {}", toString());

        PinotFSSpec fsSpec = new Yaml().loadAs(_fsSpecString, PinotFSSpec.class);
        PinotConfiguration pinotFSConfig = new PinotConfiguration(fsSpec);

        PinotFSFactory.init(new PinotConfiguration(fsSpec));
        PinotFSFactory.register(fsSpec.getScheme(), fsSpec.getClassName(), pinotFSConfig);

        PinotFS pinotFS = PinotFSFactory.create(fsSpec.getScheme());

        switch (_command.toLowerCase()) {
            case "init":
                pinotFS.init(pinotFSConfig);
                LOGGER.info("Filesystem initialized.");
                break;
            case "mkdir":
                if (_sourceUri == null) {
                    LOGGER.error("URI is required for mkdir command.");
                    return false;
                }
                URI mkdirUri = URI.create(_sourceUri);
                boolean mkdirResult = pinotFS.mkdir(mkdirUri);
                LOGGER.info("Mkdir {} for {}", (mkdirResult ? "successful" : "failed"), mkdirUri);
                return mkdirResult;
            case "delete":
                if (_sourceUri == null) {
                    LOGGER.error("URI is required for delete command.");
                    return false;
                }
                URI deleteUri = URI.create(_sourceUri);
                boolean deleteResult = pinotFS.delete(deleteUri, _forceDelete);
                LOGGER.info("Delete {} for {}", (deleteResult ? "successful" : "failed"), deleteUri);
                return deleteResult;
            case "move":
                if (_sourceUri == null || _destUri == null) {
                    LOGGER.error("Source URI and Destination URI are required for move command.");
                    return false;
                }
                URI moveSrcUri = URI.create(_sourceUri);
                URI moveDstUri = URI.create(_destUri);
                boolean moveResult = pinotFS.move(moveSrcUri, moveDstUri, _overwrite);
                LOGGER.info("Move {} from {} to {}", (moveResult ? "successful" : "failed"), moveSrcUri, moveDstUri);
                return moveResult;
            case "copy":
                if (_sourceUri == null || _destUri == null) {
                    LOGGER.error("Source URI and Destination URI are required for copy command.");
                    return false;
                }
                URI copySrcUri = URI.create(_sourceUri);
                URI copyDstUri = URI.create(_destUri);
                boolean copyResult = pinotFS.copy(copySrcUri, copyDstUri);
                LOGGER.info("Copy {} from {} to {}", (copyResult ? "successful" : "failed"), copySrcUri, copyDstUri);
                return copyResult;
            case "exists":
                if (_sourceUri == null) {
                    LOGGER.error("URI is required for exists command.");
                    return false;
                }
                URI existsUri = URI.create(_sourceUri);
                boolean existsResult = pinotFS.exists(existsUri);
                LOGGER.info("Exists {} for {}", (existsResult ? "true" : "false"), existsUri);
                return existsResult;
            case "length":
                if (_sourceUri == null) {
                    LOGGER.error("URI is required for length command.");
                    return false;
                }
                URI lengthUri = URI.create(_sourceUri);
                long length = pinotFS.length(lengthUri);
                LOGGER.info("Length of {} is {} bytes", lengthUri, length);
                return true;
            case "listfiles":
                if (_sourceUri == null) {
                    LOGGER.error("URI is required for listFiles command.");
                    return false;
                }
                URI listFilesUri = URI.create(_sourceUri);
                String[] files = pinotFS.listFiles(listFilesUri, _recursive);
                LOGGER.info("Files under {}:", listFilesUri);
                for (String file : files) {
                    LOGGER.info(file);
                }
                return true;
            case "copytolocal":
                if (_sourceUri == null || _filePath == null) {
                    LOGGER.error("Source URI and File Path are required for copyToLocal command.");
                    return false;
                }
                URI copyToLocalUri = URI.create(_sourceUri);
                File dstFile = new File(_filePath);
                pinotFS.copyToLocalFile(copyToLocalUri, dstFile);
                LOGGER.info("Copied to local file: {}", dstFile.getAbsolutePath());
                return true;
            case "copyfromlocal":
                if (_filePath == null || _destUri == null) {
                    LOGGER.error("File Path and Destination URI are required for copyFromLocal command.");
                    return false;
                }
                File srcFile = new File(_filePath);
                URI copyFromLocalUri = URI.create(_destUri);
                pinotFS.copyFromLocalFile(srcFile, copyFromLocalUri);
                LOGGER.info("Copied from local file: {} to {}", srcFile.getAbsolutePath(), copyFromLocalUri);
                return true;
            case "isdirectory":
                if (_sourceUri == null) {
                    LOGGER.error("URI is required for isDirectory command.");
                    return false;
                }
                URI isDirectoryUri = URI.create(_sourceUri);
                boolean isDirectoryResult = pinotFS.isDirectory(isDirectoryUri);
                LOGGER.info("{} is {}", isDirectoryUri, (isDirectoryResult ? "a directory" : "not a directory"));
                return isDirectoryResult;
            case "lastmodified":
                if (_sourceUri == null) {
                    LOGGER.error("URI is required for lastModified command.");
                    return false;
                }
                URI lastModifiedUri = URI.create(_sourceUri);
                long lastModified = pinotFS.lastModified(lastModifiedUri);
                LOGGER.info("Last modified time of {} is {} ms", lastModifiedUri, lastModified);
                return true;
            case "touch":
                if (_sourceUri == null) {
                    LOGGER.error("URI is required for touch command.");
                    return false;
                }
                URI touchUri = URI.create(_sourceUri);
                boolean touchResult = pinotFS.touch(touchUri);
                LOGGER.info("Touch {} for {}", (touchResult ? "successful" : "failed"), touchUri);
                return touchResult;
            case "open":
                if (_sourceUri == null) {
                    LOGGER.error("URI is required for open command.");
                    return false;
                }
                URI openUri = URI.create(_sourceUri);
                try (InputStream inputStream = pinotFS.open(openUri)) {
                    // You can process the InputStream as needed here
                    LOGGER.info("Opened {} successfully", openUri);
                }
                return true;
            default:
                LOGGER.error("Unknown command: {}", _command);
                printUsage();
                return false;
        }
        return true;
    }


    public void printUsage() {
        CommandLine.usage(new PinotFSCommand(), System.out);
    }
}

