package com.example.drill;

import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.proto.UserBitShared.QueryType;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class DrillQuerySubmitter {

  /**
   * Runnable class that runs the given query against the given client and waits for failure or completion.
   */
  private static class QueryRunnable implements Runnable {

    private final DrillClient client;
    private final String query;
    private final CountDownLatch trigger;

    public QueryRunnable(final DrillClient client, final String query, final CountDownLatch trigger) {
      this.client = client;
      this.query = query;
      this.trigger = trigger;
    }

    @Override
    public void run() {
      final WaitUntilCompleteListener listener = new WaitUntilCompleteListener();

      client.runQuery(QueryType.SQL, query, listener);

      final QueryState state = listener.waitUntilComplete();
      final String queryId = listener.getQueryId();
      final long completionId = trigger.getCount();

      if (state == QueryState.FAILED) {
        final String errorMessage = listener.getErrorMessage();
        System.out.println(String.format("%d: Query (%s) FAILED (%s...)", completionId, queryId,
            errorMessage.substring(0, Math.min(50, errorMessage.length()))));
      } else {
        System.out.println(String.format("%d: Query (%s) %s", completionId, queryId, state));
      }

      trigger.countDown();
    }
  }

  private static List<String> getQueries(final String path) {
    System.out.println(String.format("Reading queries from %s", path));
    final List<String> queries = new ArrayList<String>();
    try {
      final File directory = new File(path);
      final File[] files = directory.listFiles();
      assert files != null;
      for (final File file : files) {
        if (file.isFile()) {
          queries.add(new String(Files.readAllBytes(file.toPath())));
        }
      }
    } catch (final Exception e) {
      System.out.println("Error while reading files: " + e.getMessage());
      System.exit(-1);
    }
    return queries;
  }

  public static void main(final String[] args) {
    if (args.length != 5) {
      System.out.println("Drill query submitter." +
        "\nArguments: \t<connection_url> <setup_queries_directory> <num_threads> <queries_directory> <sleep_time_ms>\n");
      System.exit(-1);
    }

    final String connectUrl = args[0]; //"zk_address:zk_port/zk_root/cluster_id"; e.g. "10.10.30.207:5181/drill/cluster"
    final List<String> setupQueries = getQueries(args[1]);
    final int numThreads = Integer.parseInt(args[2]);
    final List<String> queries = getQueries(args[3]);
    final int sleepTime = Integer.parseInt(args[4]);

    System.out.println("Connecting to drillbit..");
    final DrillClient client = new DrillClient();
    try {
      client.connect(connectUrl, null);
    } catch (final Exception e) {
      System.out.println("Failed trying to connect to Drill cluster: " + e.getMessage());
      System.exit(-1);
    }

    System.out.println("Running setup queries..");
    final CountDownLatch setupTrigger = new CountDownLatch(setupQueries.size());
    for (final String setupQuery : setupQueries) {
      new Thread(new QueryRunnable(client, setupQuery, setupTrigger)).start();
    }
    try {
      setupTrigger.await();
    } catch (final InterruptedException e) {
      System.out.println("Interrupted while waiting for setup to finish: " + e.getMessage());
      client.close();
      System.exit(-1);
    }

    final int numQueries = numThreads * queries.size();
    final CountDownLatch trigger = new CountDownLatch(numQueries);
    System.out.println(String.format("Running %d queries..", numQueries));
    try {
      int j = 0;
      for (int i = 0; i < numQueries; i++) {
        try {
          TimeUnit.MILLISECONDS.sleep(sleepTime);
        } catch (final InterruptedException e) {
          System.out.println("Interrupted while sleeping. Submitting another query.");
        }
        new Thread(new QueryRunnable(client, queries.get(j), trigger)).start();
        j = ++j == queries.size() ? 0 : j; // wrap around
      }

      trigger.await();
    } catch (final InterruptedException e) {
      System.out.println("Interrupted while waiting for queries to finish: " + e.getMessage());
    } finally {
      System.out.println("Closing client.");
      client.close();
    }
  }
}
