import java.util.zip.ZipFile

def expected = [
    'PINOT-INF/classes/org/apache/pinot/it/Simple.class',
    "PINOT-INF/lib/commons-lang3-${commonslang3_version}.jar"
] as Set

def entries = new File(basedir,'target/simple-assembly-0.0.1-SNAPSHOT-plugin.zip').with {
  f ->
    def archive = new ZipFile(f)
    def result = archive.entries().findAll{ !it.directory }.collect { it.name } as Set
    archive.close()
    return result
}

assert entries == expected
