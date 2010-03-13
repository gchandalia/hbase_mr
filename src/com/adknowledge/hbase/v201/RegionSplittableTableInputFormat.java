package com.adknowledge.hbase.v201;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.hbase.mapred.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;


/**
 * Converts Hbase tabular data into a format that can be consumed by Map/Reduce.
 * <p>
 * Modified version of {@link com.adknowledge.hbase.v201.TableInputFormat}. Each
 * region is split into a configurable number of splits so that there can be
 * multiple map tasks per region, see
 * {@link RegionSplittableTableInputFormat#MAP_SPLITS_PER_REGION}.
 */
public class RegionSplittableTableInputFormat extends TableInputFormat {
  /**
   * number of map splits per region
   */
  public static final String MAP_SPLITS_PER_REGION = "map.splits.per.region";
  private static final Log LOG =
      LogFactory.getLog(RegionSplittableTableInputFormat.class);

  int numSplitsPerRegion;

  @Override
  public void configure(JobConf job) {
    super.configure(job);
    numSplitsPerRegion = job.getInt(MAP_SPLITS_PER_REGION, 2);
  }

  /**
   * Returns the input splits.
   * <p>
   * Number of splits is equal to <tt>(numRegions-1)*numSplitsPerRegion + 1</tt>
   * where numRegions is the number of regions and numSplitsPerRegion is the
   * number of desired map tasks per region. If numSplitsPerRegion = 1 then
   * the number of splits is simply equal to the number of regions.
   */
  @SuppressWarnings("boxing")
  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    if (numSplitsPerRegion == 1) {
      return super.getSplits(job, numSplits);
    }
    if (table == null) {
      throw new IOException("No table was provided");
    }
    if (inputColumns == null || inputColumns.length == 0) {
      throw new IOException("Expecting at least one column");
    }
    waitForRegionsToStabilize(table);
    Pair<byte[][], byte[][]> startEndKeys = table.getStartEndKeys();
    byte[][] regionStartKeys = startEndKeys.getFirst();
    byte[][] regionEndKeys = startEndKeys.getSecond();
    // key: server, value: number of splits
    Map<String, Integer> serverSplitCounts = new TreeMap<String, Integer>();
    ArrayList<TableSplit> splits = new ArrayList<TableSplit>(
        (regionStartKeys.length-1)*numSplitsPerRegion + 1);
    for (int i = 0; i < regionStartKeys.length; ++i) {
      byte[] regionStartKey = regionStartKeys[i];
      byte[] regionEndKey = regionEndKeys[i];
      byte[][] regionSplits = null;
      // if this region is the last one
      if (i == regionStartKeys.length-1) {
        regionSplits = new byte[][]{regionStartKey, regionEndKey};
      } else {
        regionSplits =
            Bytes.split(regionStartKey, regionEndKey, numSplitsPerRegion-1);
      }
      LOG.info("region " + i + " -> " +
               Bytes.toStringBinary(regionStartKey) + ", " +
               Bytes.toStringBinary(regionEndKey));
      for (int j = 0; j < regionSplits.length-1; ++j) {
        byte[] splitStartKey = regionSplits[j];
        byte[] splitEndKey = regionSplits[j+1];
        String regionLocation = getRegionLocation(splitStartKey);
        incrCount(regionLocation, serverSplitCounts);
        TableSplit split = new TableSplit(
            table.getTableName(), splitStartKey, splitEndKey, regionLocation);
        splits.add(split);
        LOG.info("    split -> " + split);
      }
    }
    LOG.info("regions: " + regionStartKeys.length + ", splits: " +
             splits.size() + ", load: " + serverSplitCounts.toString());
    return splits.toArray(new InputSplit[splits.size()]);
  }
}
