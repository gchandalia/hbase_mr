package com.adknowledge.hbase.v201;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.NoServerForRegionException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.filter.StopRowFilter;
import org.apache.hadoop.hbase.filter.WhileMatchRowFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.mapred.TableSplit;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;


/**
 * Converts Hbase tabular data into a format that can be consumed by Map/Reduce.
 * <p>
 * Modified version of {@link org.apache.hadoop.hbase.mapreduce.TableInputFormat}.
 * <p>
 * Before assigning the regions to splits, waits for the regions to stabilize,
 * see {@link TableInputFormat#waitForRegionsToStabilize(org.apache.hadoop.hbase.client.HTable)}.
 * <p>
 * <b>Note:</b> Doesn't yet use the {@link Scan} interface.
 */
public class TableInputFormat
    implements InputFormat<ImmutableBytesWritable, Result>, JobConfigurable {
  /**
   * space delimited list of columns
   */
  public static final String COLUMN_LIST = "hbase.mapred.tablecolumns";
  private static final int[] SPLIT_RETRIES = new int[]{1, 2, 3};
  /**
   * number of seconds to wait when checking for regions stability.
   */
  static final int[] CHECK_REGION_STABLE_BACKOFF =
      new int[]{20, 16, 12, 8, 4, 2, 1};
  private static final Log LOG = LogFactory.getLog(TableInputFormat.class);

  byte[][] inputColumns;
  HTable table;
  private TableRecordReader tableRecordReader;
  private RowFilterInterface rowFilter;

  /**
   * Iterate over an HBase table data, return (Text, RowResult) pairs
   */
  protected class TableRecordReader
      implements RecordReader<ImmutableBytesWritable, Result> {
    private byte[] startRow;
    private byte[] endRow;
    private byte[] lastRow;
    private RowFilterInterface trrRowFilter;
    private ResultScanner scanner;
    private HTable htable;
    private byte[][] trrInputColumns;

    /**
     * Restart from survivable exceptions by creating a new scanner.
     *
     * @param firstRow
     * @throws IOException
     */
    public void restart(byte[] firstRow) throws IOException {
      if ((endRow != null) && (endRow.length > 0)) {
        if (trrRowFilter != null) {
          final Set<RowFilterInterface> rowFiltersSet =
              new HashSet<RowFilterInterface>();
          rowFiltersSet.add(new WhileMatchRowFilter(new StopRowFilter(endRow)));
          rowFiltersSet.add(trrRowFilter);
          Scan scan = new Scan(startRow);
          scan.addColumns(trrInputColumns);
          scanner = htable.getScanner(scan);
        } else {
          Scan scan = new Scan(firstRow, endRow);
          scan.addColumns(trrInputColumns);
          scanner = htable.getScanner(scan);
        }
      } else {
        Scan scan = new Scan(firstRow);
        scan.addColumns(trrInputColumns);
        // scan.setFilter(trrRowFilter);
        scanner = htable.getScanner(scan);
      }
      if (scanner == null) {
        throw new IOException("Scanner is null");
      }
    }

    /**
     * Build the scanner. Not done in constructor to allow for extension.
     *
     * @throws IOException
     */
    public void init() throws IOException {
      restart(startRow);
    }

    /**
     * @param htable the {@link HTable} to scan.
     */
    public void setHTable(HTable htable) {
      this.htable = htable;
    }

    /**
     * @param inputColumns the columns to be placed in {@link RowResult}.
     */
    public void setInputColumns(final byte[][] inputColumns) {
      trrInputColumns = inputColumns;
    }

    /**
     * @param startRow the first row in the split
     */
    public void setStartRow(final byte[] startRow) {
      this.startRow = startRow;
    }

    /**
     * @param endRow the last row in the split
     */
    public void setEndRow(final byte[] endRow) {
      this.endRow = endRow;
    }

    /**
     * @param rowFilter the {@link RowFilterInterface} to be used.
     */
    public void setRowFilter(RowFilterInterface rowFilter) {
      trrRowFilter = rowFilter;
    }

    public void close() {
      scanner.close();
    }

    /**
     * @return ImmutableBytesWritable
     * @see org.apache.hadoop.mapred.RecordReader#createKey()
     */
    public ImmutableBytesWritable createKey() {
      return new ImmutableBytesWritable();
    }

    /**
     * @return RowResult
     * @see org.apache.hadoop.mapred.RecordReader#createValue()
     */
    public Result createValue() {
      return new Result();
    }

    public long getPos() {
      // This should be the ordinal tuple in the range;
      // not clear how to calculate...
      return 0;
    }

    public float getProgress() {
      // Depends on the total number of tuples and getPos
      return 0;
    }

    /**
     * @param key HStoreKey as input key.
     * @param value MapWritable as input value
     * @return true if there was more data
     * @throws IOException
     */
    public boolean next(ImmutableBytesWritable key, Result value)
        throws IOException {
      Result result;
      try {
        result = scanner.next();
      } catch (UnknownScannerException e) {
        LOG.debug("recovered from " + StringUtils.stringifyException(e));
        restart(lastRow);
        scanner.next(); // skip presumed already mapped row
        result = scanner.next();
      }
      if (result != null && result.size() > 0) {
        key.set(result.getRow());
        lastRow = result.getRow();
        Writables.copyWritable(result, value);
        return true;
      }
      return false;
    }
  }

  /**
   * Creates an {@link HTable} instance with specified input columns.
   *
   * @param job
   */
  public void configure(JobConf job) {
    Path[] tableNames = FileInputFormat.getInputPaths(job);
    String colArg = job.get(COLUMN_LIST);
    colArg = colArg.replaceAll("\"", "").replaceAll("'", "");
    String[] colNames = colArg.split(" ");
    byte[][] m_cols = new byte[colNames.length][];
    for (int i = 0; i < m_cols.length; i++) {
      m_cols[i] = Bytes.toBytes(colNames[i]);
    }
    setInputColumns(m_cols);
    try {
      setHTable(new HTable(new HBaseConfiguration(job), tableNames[0].getName()));
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
    }
  }

  public void validateInput(JobConf job) throws IOException {
    // expecting exactly one path
    Path[] tableNames = FileInputFormat.getInputPaths(job);
    if (tableNames == null || tableNames.length > 1) {
      throw new IOException("expecting one table name");
    }

    // connected to table?
    if (getHTable() == null) {
      throw new IOException(
          "could not connect to table '" + tableNames[0].getName() + "'");
    }

    // expecting at least one column
    String colArg = job.get(COLUMN_LIST);
    if (colArg == null || colArg.length() == 0) {
      throw new IOException("expecting at least one column");
    }

  }

  /**
   * Builds a TableRecordReader. If no TableRecordReader was provided, uses the
   * default.
   *
   * @see org.apache.hadoop.mapred.InputFormat#getRecordReader(InputSplit, JobConf, Reporter)
   */
  public RecordReader<ImmutableBytesWritable, Result> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter) throws IOException {
    TableSplit tSplit = (TableSplit) split;
    TableRecordReader trr = this.tableRecordReader;
    // if no table record reader was provided use default
    if (trr == null) {
      trr = new TableRecordReader();
    }
    trr.setStartRow(tSplit.getStartRow());
    trr.setEndRow(tSplit.getEndRow());
    trr.setHTable(table);
    trr.setInputColumns(inputColumns);
    trr.setRowFilter(rowFilter);
    trr.init();
    return trr;
  }

  /**
   * Calculates the splits that will serve as input for the map tasks.
   * <ul>
   * Number of splits is equal to the the number of {@link HRegion}s in the table.
   *
   * @param job the map task {@link JobConf}
   * @param numSplits a hint to calculate the number of splits (mapred.map.tasks).
   * @return the input splits
   * @see org.apache.hadoop.mapred.InputFormat#getSplits(org.apache.hadoop.mapred.JobConf, int)
   */
  @SuppressWarnings("boxing")
  public InputSplit[] getSplits(
      JobConf job, @SuppressWarnings("unused") int numSplits)
      throws IOException {
    if (table == null) {
      throw new IOException("No table was provided");
    }
    if (inputColumns == null || inputColumns.length == 0) {
      throw new IOException("Expecting at least one column");
    }
    waitForRegionsToStabilize(table);
    byte[][] startKeys = table.getStartKeys();
    // key: server, value: number of splits
    Map<String, Integer> serverSplitCounts = new TreeMap<String, Integer>();
    // number of splits = number of regions in the table
    int realNumSplits = startKeys.length;
    InputSplit[] splits = new InputSplit[realNumSplits];
    int middle = startKeys.length / realNumSplits;
    int startPos = 0;
    for (int i = 0; i < realNumSplits; i++) {
      int lastPos = startPos + middle;
      lastPos = startKeys.length % realNumSplits > i ? lastPos + 1 : lastPos;
      String regionLocation = getRegionLocation(startKeys[startPos]);
      incrCount(regionLocation, serverSplitCounts);
      splits[i] = new TableSplit(
          table.getTableName(), startKeys[startPos],
          ((i + 1) < realNumSplits) ? startKeys[lastPos]
          : HConstants.EMPTY_END_ROW,
          regionLocation);
      LOG.info("split -> " + splits[i]);
      startPos = lastPos;
    }
    LOG.info("total splits: " + splits.length + ", load: " +
             serverSplitCounts.toString());
    return splits;
  }

  void incrCount(String regionLocation, Map<String, Integer> serverSplitCounts) {
    if (!serverSplitCounts.containsKey(regionLocation)) {
      serverSplitCounts.put(regionLocation, 1);
    }
    serverSplitCounts.put(
        regionLocation, serverSplitCounts.get(regionLocation)+1);
  }

  String getRegionLocation(byte[] row) throws IOException {
    for (int r : SPLIT_RETRIES) {
      try {
        return table.getRegionLocation(row).getServerAddress().getHostname();
      } catch (NoServerForRegionException nsfre) {
        if (r == SPLIT_RETRIES[SPLIT_RETRIES.length-1]) {
          throw new IOException(nsfre);
        }
        LOG.warn("attempt: " + r + ", " + nsfre);
        try {
          Thread.sleep(5000);
        } catch (InterruptedException ie) {
        }
      }
    }
    return null;
  }

  /**
   * Waits for all regions to split under heavy load, <b>exits only when the
   * number of regions stays stable for a certain period of time</b>.
   * <p>
   * This method takes a minimum of
   * <tt>SLEEP_TIME*sum(CHECK_REGION_STABLE_BACKOFF)</tt>.
   */
  static void waitForRegionsToStabilize(HTable table) throws IOException {
    int prevNumRegions = -1;
    int counter = 0;
    while (counter < CHECK_REGION_STABLE_BACKOFF.length) {
      try {
        Thread.sleep(CHECK_REGION_STABLE_BACKOFF[counter]*1000);
      } catch (InterruptedException ie) {
      }
      try {
        byte[][] startKeys = table.getStartKeys();
        if (startKeys.length == prevNumRegions) {
          ++counter;
        } else {
          prevNumRegions = startKeys.length;
          counter = 0;
        }
        LOG.info("total regions diff: " + (startKeys.length-prevNumRegions) +
                 ", counter: " + counter);
      } catch (NullPointerException npe) {
        LOG.info("waiting for .META. to be updated...");
      } catch (IOException ioe) {
        LOG.warn("Unknown IOException!");
      }
    }
  }

  /**
   * @param inputColumns to be passed in {@link RowResult} to the map task.
   */
  protected void setInputColumns(byte[][] inputColumns) {
    this.inputColumns = inputColumns;
  }

  /**
   * Allows subclasses to get the {@link HTable}.
   */
  protected HTable getHTable() {
    return table;
  }

  /**
   * Allows subclasses to set the {@link HTable}.
   *
   * @param table to get the data from
   */
  protected void setHTable(HTable table) {
    this.table = table;
  }

  /**
   * Allows subclasses to set the {@link TableRecordReader}.
   *
   * @param tableRecordReader to provide other {@link TableRecordReader}
   *        implementations.
   */
  protected void setTableRecordReader(TableRecordReader tableRecordReader) {
    this.tableRecordReader = tableRecordReader;
  }

  /**
   * Allows subclasses to set the {@link RowFilterInterface} to be used.
   *
   * @param rowFilter
   */
  protected void setRowFilter(RowFilterInterface rowFilter) {
    this.rowFilter = rowFilter;
  }
}
