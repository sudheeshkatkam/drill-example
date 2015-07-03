package com.example.drill;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.rpc.user.ConnectionThrottle;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;

import java.util.concurrent.CountDownLatch;

public class DrillQuerySubmitter {

  public static final String CONNECT_URL = "10.10.30.207:5181/drill/cluster"; // "zk_address:zk_port/zk_root/cluster_id"

  public static void main (final String[] args) {
    if (args.length != 1) {
      System.out.println("Invalid usage.");
      System.exit(-1);
    }

    final DrillClient client = new DrillClient();
    try {
      client.connect(CONNECT_URL, null);
    } catch (final Exception e) {
      System.out.println("Failed trying to connect to Drill cluster: " + e.getMessage());
      System.exit(-1);
    }

    final CountDownLatch latch = new CountDownLatch(1);
    final UserResultsListener listener = new UserResultsListener() {

      @Override
      public void queryIdArrived(final QueryId queryId) {
        System.out.println("Query Id arrived: " + QueryIdHelper.getQueryId(queryId));
      }

      @Override
      public void submissionFailed(final UserException e) {
        System.out.println("Query failed with exception: " + e.getMessage());
        latch.countDown();
      }

      @Override
      public void dataArrived(final QueryDataBatch batch, final ConnectionThrottle connection) {
        batch.release();
      }

      @Override
      public void queryCompleted(final QueryState state) {
        System.out.println("Final state: " + state);
        latch.countDown();
      }
    };

    System.out.println("> " + args[0]);
    client.runQuery(QueryType.SQL, args[0], listener);
    try {
      latch.await();
    } catch (final InterruptedException e) {
      System.err.println("Interrupted while waiting for results: " + e.getMessage());
    } finally {
      client.close();
    }
  }
}
