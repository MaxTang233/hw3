package edu.berkeley.cs186.database.query;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

public class PNLJOperator extends JoinOperator {

  public PNLJOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource,
          rightSource,
          leftColumnName,
          rightColumnName,
          transaction,
          JoinType.PNLJ);

  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new PNLJIterator();
  }


  public int estimateIOCost() throws QueryPlanException {
	    //does nothing
	    return 0;
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class PNLJIterator extends JoinIterator {
    /**
     * Some member variables are provided for guidance, but there are many possible solutions.
     * You should implement the solution that's best for you, using any member variables you need.
     * You're free to use these member variables, but you're not obligated to.
     */

    private Iterator<Page> leftIterator = null;
    private Iterator<Page> rightIterator = null;
    private BacktrackingIterator<Record> leftRecordIterator = null;
    private BacktrackingIterator<Record> rightRecordIterator = null;
    private Record leftRecord = null;
    private Record nextRecord = null;
    private Record rightRecord;

    public PNLJIterator() throws QueryPlanException, DatabaseException {
      super();
      this.rightIterator = PNLJOperator.this.getPageIterator(this.getRightTableName());
      this.leftIterator = PNLJOperator.this.getPageIterator(this.getLeftTableName());
      rightIterator.next();
      leftIterator.next();
      this.rightRecordIterator = PNLJOperator.this.getBlockIterator(this.getRightTableName(), rightIterator, 1);
      this.leftRecordIterator = PNLJOperator.this.getBlockIterator(this.getLeftTableName(), leftIterator, 1);

      this.nextRecord = null;

      this.leftRecord = leftRecordIterator.hasNext() ? leftRecordIterator.next() : null;
      this.rightRecord = rightRecordIterator.hasNext() ? rightRecordIterator.next() : null;

      if (leftRecord != null) {
          leftRecordIterator.mark();
      }
      if (rightRecord != null) {
          rightRecordIterator.mark();
      }
      else {
          return;
      }

      try {
          fetchNextRecord();
      } catch (DatabaseException e) {
          this.nextRecord = null;
      }
    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
        return nextRecord != null;
    }


      private void resetRightRecord() throws DatabaseException {
          rightIterator = PNLJOperator.this.getPageIterator(this.getRightTableName());
          assert(rightIterator.hasNext());
          rightIterator.next();
          rightRecordIterator = PNLJOperator.this.getBlockIterator(this.getRightTableName(), rightIterator, 1);
          rightRecord = rightRecordIterator.next();
          rightRecordIterator.mark();
      }


      private void fetchNextRecord() throws DatabaseException {
          if (this.leftRecord == null) {
              throw new DatabaseException("No new record to fetch");
          } else {
              nextRecord = null;
              while (!hasNext()) {
                  if (this.rightRecord != null) {
                      DataBox leftJoinValue = this.leftRecord.getValues().get(PNLJOperator.this.getLeftColumnIndex());
                      DataBox rightJoinValue = rightRecord.getValues().get(PNLJOperator.this.getRightColumnIndex());
                      if (leftJoinValue.equals(rightJoinValue)) {
                          List<DataBox> leftValues = new ArrayList<>(this.leftRecord.getValues());
                          List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
                          leftValues.addAll(rightValues);
                          this.nextRecord = new Record(leftValues);
                      }
                      this.rightRecord = rightRecordIterator.hasNext() ? rightRecordIterator.next() : null;
                  } else {
                      if (!leftRecordIterator.hasNext() && !rightRecordIterator.hasNext() && !rightIterator.hasNext() && !leftIterator.hasNext()) {
                          throw new DatabaseException("no more");
                      } else if (!leftRecordIterator.hasNext() && !rightRecordIterator.hasNext() && !rightIterator.hasNext() && leftIterator.hasNext()) {
                          leftRecordIterator = PNLJOperator.this.getBlockIterator(this.getLeftTableName(), leftIterator, 1);
                          leftRecord = leftRecordIterator.next();
                          leftRecordIterator.mark();
                          resetRightRecord();
                      } else if (!leftRecordIterator.hasNext() && !rightRecordIterator.hasNext() && rightIterator.hasNext()) {
                          leftRecordIterator.reset();
                          this.leftRecord = leftRecordIterator.hasNext() ? leftRecordIterator.next() : null;
                          leftRecordIterator.mark();

                          rightRecordIterator = PNLJOperator.this.getBlockIterator(this.getRightTableName(), rightIterator, 1);
                          this.rightRecord = rightRecordIterator.hasNext() ? rightRecordIterator.next() : null;
                          rightRecordIterator.mark();
                      } else {
                          leftRecord = leftRecordIterator.next();

                          rightRecordIterator.reset();
                          rightRecord = rightRecordIterator.next();
                          rightRecordIterator.mark();
                      }


                  }
              }
          }

      }

    /**
     * Yields the next record of this iterator.
     *
     * @return the next Record
     * @throws NoSuchElementException if there are no more Records to yield
     */
    public Record next() {
        if (!this.hasNext()) {
            throw new NoSuchElementException();
        }

        Record nextRecord = this.nextRecord;
        try {
            this.fetchNextRecord();
        } catch (DatabaseException e) {
            this.nextRecord = null;
        }
        return nextRecord;
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}

