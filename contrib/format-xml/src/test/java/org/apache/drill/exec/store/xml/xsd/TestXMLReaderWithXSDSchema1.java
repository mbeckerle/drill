package org.apache.drill.exec.store.xml.xsd;

import org.apache.drill.categories.RowSetTest;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.util.DrillFileUtils;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.xml.XMLFormatConfig;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryBuilder;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;

import static org.apache.drill.exec.proto.UserBitShared.DrillPBError.ErrorType.DATA_READ;
import static org.apache.drill.test.rowSet.RowSetUtilities.*;
import static org.junit.Assert.*;

@Category(RowSetTest.class)
public class TestXMLReaderWithXSDSchema1 extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));

    cluster.defineFormat("cp", "xml", new XMLFormatConfig(null, 1));
  }

  /**
   * Positive test: Shows that the Drill schema INT definition works when the XML data contains an xs:int
   * value of 1.
   */
  @Test
  public void test1IntOKWithDrillSchema() throws Exception {
    String sql = "SELECT int1 FROM table(cp.`xml/test1_1.xml` (type => 'xml', schema => 'inline=(`int1` INT)'))";
    RowSet results = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("int1", TypeProtos.MinorType.INT)
        .build();

    RowSet expected = client.rowSetBuilder(expectedSchema)
        .addRow(1)
        .build();

    assertEquals(1, results.rowCount());
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  /**
   * Negative test: Shows that the Drill schema INT definition detects non-Int when the data
   * value of "A".
   */
  @Test
  public void test1IntBadWithDrillSchema() throws Exception {
    String sql = "SELECT int1 FROM table(cp.`xml/test1_A.xml` (type => 'xml', schema => 'inline=(`int1` INT)'))";
    QueryBuilder qb = client.queryBuilder();
    QueryBuilder q = qb.sql(sql);
    try {
      q.rowSet();
    } catch (UserException e) {
      assertEquals(DATA_READ, e.getErrorType());
      String m = e.getMessage().toLowerCase();
      assertTrue(m.contains("error parsing file"));
      assertTrue(m.contains("\"A\"".toLowerCase()));
    }
  }

  /**
   * Positive test: Shows that with no schema at all, the query is fine when the int1 element has
   * value "A".
   */
  @Test
  public void test1IntWithoutSchema() throws Exception {
    String sql = "SELECT int1 FROM cp.`xml/test1_A.xml`";
    RowSet results = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("int1", TypeProtos.MinorType.VARCHAR)
        .build();

    RowSet expected = client.rowSetBuilder(expectedSchema)
        .addRow("A")
        .build();

    assertEquals(1, results.rowCount());
    new RowSetComparison(expected).verifyAndClearAll(results);
  }
}
