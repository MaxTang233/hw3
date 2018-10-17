package edu.berkeley.cs186.database.query;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordIterator;
import edu.berkeley.cs186.database.table.Schema;

public class SortMergeOperator extends JoinOperator {

  public SortMergeOperator(QueryOperator leftSource,
           QueryOperator rightSource,
           String leftColumnName,
           String rightColumnName,
           Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new SortMergeOperator.SortMergeIterator();
  }

  public int estimateIOCost() throws QueryPlanException {
    //does nothing
    return 0;
  }   

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   *
   * Before proceeding, you should read and understand SNLJOperator.java
   *    You can find it in the same directory as this file.
   *
   * Word of advice: try to decompose the problem into distinguishable sub-problems.
   *    This means you'll probably want to add more methods than those given (Once again,
   *    SNLJOperator.java might be a useful reference).
   * 
   */
  private class SortMergeIterator extends JoinIterator {
    /** 
    * Some member variables are provided for guidance, but there are many possible solutions.
    * You should implement the solution that's best for you, using any member variables you need.
    * You're free to use these member variables, but you're not obligated to.
    */

    private String leftTableName;
    private String rightTableName;
    private RecordIterator leftIterator;
    private RecordIterator rightIterator;
    private Record leftRecord;
    private Record nextRecord;
    private Record rightRecord;
    private boolean marked;
    private LR_RecordComparator lr_recordComparator = new LR_RecordComparator();

    public SortMergeIterator() throws QueryPlanException, DatabaseException {
      super();
      this.leftTableName = this.getLeftTableName();
      this.rightTableName = this.getRightTableName();
      this.leftIterator = SortMergeOperator.this.getRecordIterator(this.leftTableName);
      SortOperator s = new SortOperator(SortMergeOperator.this.getTransaction(),this.leftTableName, new LeftRecordComparator());
      String sortedLeftTable = s.sort();
      this.leftIterator = SortMergeOperator.this.getTransaction().getRecordIterator(sortedLeftTable);

      this.rightIterator = SortMergeOperator.this.getRecordIterator(this.rightTableName);
      s = new SortOperator(SortMergeOperator.this.getTransaction(),this.rightTableName, new RightRecordComparator());
      String sortedRightTable = s.sort();
      this.rightIterator = SortMergeOperator.this.getTransaction().getRecordIterator(sortedRightTable);


      this.nextRecord = null;

      this.leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
      this.rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;

      this.marked = false;

      // We mark the first record so we can reset to it when we advance the left record.
      if (rightRecord != null) {
        rightIterator.mark();
      }
      else return;

      try {
        fetchNextRecord();
      } catch (DatabaseException e) {
        this.nextRecord = null;
      }
    }

    private void fetchNextRecord() throws DatabaseException {
      if (this.leftRecord == null) throw new DatabaseException("No new record to fetch");
      this.nextRecord = null;
      do {
        if (!marked && rightRecord != null) {
          while (this.lr_recordComparator.compare(this.leftRecord, this.rightRecord) < 0) {
            this.leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
            if (this.leftRecord == null) {
              throw new DatabaseException("all done");
            }
          }
          while (this.lr_recordComparator.compare(this.leftRecord, this.rightRecord) > 0) {
            this.rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
          }
          rightIterator.mark();
          marked = true;
        }
        if (rightRecord != null && this.lr_recordComparator.compare(this.leftRecord, this.rightRecord) == 0) {
          List<DataBox> leftValues = new ArrayList<>(this.leftRecord.getValues());
          List<DataBox> rightValues = new ArrayList<>(this.rightRecord.getValues());
          leftValues.addAll(rightValues);
          this.rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
          this.nextRecord = new Record(leftValues);

        } else {
          this.rightIterator.reset();
          rightRecord = rightIterator.next();
          this.leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
          if (this.leftRecord == null) {
            throw new DatabaseException("all done");
          }
          marked = false;
        }
      } while (!hasNext());
    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      return this.nextRecord != null;
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


    private class LeftRecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
      }
    }

    private class RightRecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
      }
    }

    /**
    * Left-Right Record comparator
    * o1 : leftRecord
    * o2: rightRecord
    */
    private class LR_RecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
      }
    }
  }
}