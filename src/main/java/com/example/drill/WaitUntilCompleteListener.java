package com.example.drill;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.rpc.user.ConnectionThrottle;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;

import java.util.concurrent.CountDownLatch;

public class WaitUntilCompleteListener implements UserResultsListener {

  private final CountDownLatch latch = new CountDownLatch(1);

  private String queryId;

  private QueryState state = QueryState.PENDING;

  private String errorMessage;

  public String getQueryId() {
    return queryId;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  @Override
  public void queryIdArrived(final UserBitShared.QueryId queryId) {
    this.queryId = QueryIdHelper.getQueryId(queryId);
  }

  @Override
  public void submissionFailed(final UserException e) {
    errorMessage = e.getMessage();
    state = QueryState.FAILED;
    latch.countDown();
  }

  @Override
  public void dataArrived(final QueryDataBatch batch, final ConnectionThrottle connection) {
    batch.release();
  }

  @Override
  public void queryCompleted(final QueryState state) {
    this.state = state;
    latch.countDown();
  }

  public QueryState waitUntilComplete() {
    try {
      latch.await();
    } catch (final InterruptedException e) {
      errorMessage = "Interrupted while waiting for results: " + e.getMessage();
    }
    return state;
  }
}
