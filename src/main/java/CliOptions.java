import java.util.List;

import com.beust.jcommander.Parameter;
import com.google.common.collect.Lists;

public class CliOptions {

	@Parameter
	public List<String> parameters = Lists.newArrayList();

	@Parameter(names = { "-seeds" }, description = "Seeds to discover cassandra Ring")
	public String seeds = "localhost";

	@Parameter(names = "-size", description = "Size of the file to upload")
	public Integer size = 1024 * 1024; // 1 MB

	@Parameter(names = "-file", description = "Upload file instead of random bytes")
	public String file;

	@Parameter(names = "-file-name", description = "Name of file to write to cassandra")
	public String fileName = "chunked-file";

	@Parameter(names = "-chunk-size", description = "Size of chunks in bytes")
	public Integer chunkSize = 1024 * 512; // 512 Kb

	@Parameter(names = "-c", description = "Concurrency")
	public Integer concurrency = Runtime.getRuntime().availableProcessors();

	@Parameter(names = "-cluster", description = "Name of cassandra cluster")
	public String custerName = "Test Cluster";

	@Parameter(names = "-keyspace", description = "Name of cassandra keyspace")
	public String keyspaceName = "Test";

	@Parameter(names = "-cf", description = "Name of cassandra column family")
	public String columnFamily = "Test";

	@Parameter(names = "-h", description = "Print usage")
	public boolean help = false;

}
