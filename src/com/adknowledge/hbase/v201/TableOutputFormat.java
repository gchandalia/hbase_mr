package com.adknowledge.hbase.v201;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * Writes the output of Map/Reduce directly to Hbase.
 * <p>
 * Modified version of {@link org.apache.hadoop.hbase.mapreduce.TableOutputFormat}.
 */
public final class TableOutputFormat extends FileOutputFormat<NullWritable, Put> {
  /** JobConf parameter that specifies the output table */
  public static final String OUTPUT_TABLE = "hbase.mapred.outputtable";
  private final Log LOG = LogFactory.getLog(TableOutputFormat.class);
  private static final int[] COMMIT_RETRIES = new int[]{1, 2, 3, 4, 5, 6};

  /**
   * Convert Reduce output (key, value) to (HStoreKey, KeyedDataArrayWritable)
   * and write to an HBase table
   */
  protected static class TableRecordWriter
      implements RecordWriter<NullWritable, Put> {
    private HTable m_table;

    /**
     * Instantiate a TableRecordWriter with the HBase HClient for writing.
     *
     * @param table
     */
    public TableRecordWriter(HTable table) {
      m_table = table;
    }

    public void close(Reporter reporter) throws IOException {
      for (int i : COMMIT_RETRIES) {
        try {
          m_table.flushCommits();
          return;
        } catch (RetriesExhaustedException ree) {
          if (i == COMMIT_RETRIES[COMMIT_RETRIES.length-1]) {
            throw new IOException(ree);
          }
          LOG.error("flush attempt: " + i + ", " + ree);
          try {
            Thread.sleep(7000);
          } catch (InterruptedException ie) {
          }
        }
      }
    }

    public void write(@SuppressWarnings("unused") NullWritable key, Put value)
        throws IOException {
      try {
        putWithRetry(value);
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }

    private void putWithRetry(Put put) throws InterruptedException, IOException {
      for (int i : COMMIT_RETRIES) {
        try {
          m_table.put(put);
          return;
        } catch (RetriesExhaustedException ree) {
          LOG.error("put attempt: " + i + ", " + ree);
          Thread.sleep(7000);
        }
      }
      LOG.error("put failed (" + COMMIT_RETRIES + " attempts), skipping row: " +
                Bytes.toString(put.getRow()));
    }

    private final Log LOG = LogFactory.getLog(TableRecordWriter.class);
  }

  @SuppressWarnings("unchecked")
  public RecordWriter<NullWritable, Put> getRecordWriter(
      FileSystem ignored, JobConf job, String name, Progressable progress)
      throws IOException {
    // expecting exactly one path
    String tableName = job.get(OUTPUT_TABLE);
    HTable table = null;
    try {
      table = new HTable(new HBaseConfiguration(job), tableName);
    } catch(IOException e) {
      LOG.error(e);
      throw e;
    }
    table.setAutoFlush(false);
    return new TableRecordWriter(table);
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job)
      throws FileAlreadyExistsException, InvalidJobConfException, IOException {
    String tableName = job.get(OUTPUT_TABLE);
    if(tableName == null) {
      throw new IOException("Must specify table name");
    }
  }
}
