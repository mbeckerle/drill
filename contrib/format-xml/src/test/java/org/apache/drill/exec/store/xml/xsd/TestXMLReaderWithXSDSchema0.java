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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(RowSetTest.class)
public class TestXMLReaderWithXSDSchema0 extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));

    cluster.defineFormat("cp", "xml", new XMLFormatConfig(null, 1));
  }

  /**
   * Negative test: Shows that the XSD schema can be used to detect that the rows returned
   * have the wrong type because the XSD schema isn't being used to convert the data
   * when the XML is read.
   */
  @Test
  public void test1IntBadWithXSDSchema() throws Exception {
    File xsd = DrillFileUtils.getResourceAsFile("/xsd/test1x.xsd");
    //
    // When Drill infers a schema from an instance of XML, there is no root element.
    // Our XSD schema creator isn't compatibile with that
    // TBD: fix that
    // so we have to move down past the root element to its contents.
    // which always has an "attributes" child element, and whatever other children.
    TupleMetadata expectedSchema = DrillXSDSchemaUtils.getSchema(xsd.getPath()).metadata("test1").tupleSchema();

    String sql = "SELECT * FROM cp.`xml/test1_1_x2.xml`";
    QueryBuilder qb = client.queryBuilder();
    QueryBuilder q = qb.sql(sql);
    RowSet results = q.rowSet();

    RowSet expected = client.rowSetBuilder(expectedSchema)
        .addRow(objArray(2), 1)
        .build();

    assertEquals(1, results.rowCount());

    try {
    new RowSetComparison(expected).verifyAndClearAll(results);
    } catch (AssertionError e) {
      String m = e.getMessage().toLowerCase();
      assertTrue(m.contains("schemas don't match"));
    }
  }
}
