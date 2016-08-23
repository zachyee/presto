/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestRemoveIdentityCasts
{
    private final LocalQueryRunner queryRunner;

    public TestRemoveIdentityCasts()
    {
        this.queryRunner = new LocalQueryRunner(testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .build());

        queryRunner.createCatalog(queryRunner.getDefaultSession().getCatalog().get(),
                new TpchConnectorFactory(queryRunner.getNodeManager(), 1),
                ImmutableMap.<String, String>of());
    }

    @Test
    public void testRemoveIdentityCastColumnData()
    {
        @Language("SQL") String sql = "SELECT CAST(nationkey AS bigint) FROM nation";
        PlanMatchPattern pattern = anyTree(
                project(anyTree()).withAssignment("Cast")
        );
        assertNotPlan(sql, pattern);
    }

    @Test
    public void testKeepNonIdentityCastColumnData()
    {
        @Language("SQL") String sql = "SELECT CAST(nationkey AS smallint) FROM nation";
        PlanMatchPattern pattern = anyTree(
                project(anyTree()).withAssignment("Cast")
        );
        assertPlan(sql, pattern);
    }

    @Test
    public void testRemoveIdentityCastParameterizedColumnData()
    {
        @Language("SQL") String sql = "SELECT CAST(name AS varchar(25)) FROM nation";
        PlanMatchPattern pattern = anyTree(
                project(anyTree()).withAssignment("Cast")
        );
        assertNotPlan(sql, pattern);
    }

    @Test
    public void testKeepNonIdentityCastParameterizedColumnData()
    {
        @Language("SQL") String sql = "SELECT CAST(name AS varchar(30)) FROM nation";
        PlanMatchPattern pattern = anyTree(
                project(anyTree()).withAssignment("Cast")
        );
        assertPlan(sql, pattern);
    }

    @Test
    public void testKeepNonIdentityCastDefaultParameterizedColumnData()
    {
        @Language("SQL") String sql = "SELECT CAST(name AS varchar) FROM nation";
        PlanMatchPattern pattern = anyTree(
                project(anyTree()).withAssignment("Cast")
        );
        assertPlan(sql, pattern);
    }

    @Test
    public void testRemoveIdentityCastImplicitBooleanData()
    {
        @Language("SQL") String sql = "SELECT CAST(boolean 'true' AS boolean)";
        PlanMatchPattern pattern = anyTree(
                project(anyTree()).withAssignment("Cast")
        );
        assertNotPlan(sql, pattern);
    }

    @Test
    public void testRemoveIdentityCastImplicitBigintData()
    {
        @Language("SQL") String sql = "SELECT CAST(bigint '5' AS bigint)";
        PlanMatchPattern pattern = anyTree(
                project(anyTree()).withAssignment("Cast")
        );
        assertNotPlan(sql, pattern);
    }

    @Test
    public void testRemoveIdentityCastImplicitDoubleDate()
    {
        @Language("SQL") String sql = "SELECT CAST(double '1.23' AS double)";
        PlanMatchPattern pattern = anyTree(
                project(anyTree()).withAssignment("Cast")
        );
        assertNotPlan(sql, pattern);
    }

    @Test
    public void testRemoveIdentityCastImplicitDecimalData()
    {
        @Language("SQL") String sql = "SELECT cast(decimal '10.354' AS decimal)";
        PlanMatchPattern pattern = anyTree(
                project(anyTree()).withAssignment("Cast")
        );
        assertNotPlan(sql, pattern);
    }

    @Test
    public void testRemoveIdentityCastImplicitVarcharData()
    {
        @Language("SQL") String sql = "SELECT CAST(varchar 'abcdef' AS varchar)";
        PlanMatchPattern pattern = anyTree(
                project(anyTree()).withAssignment("Cast")
        );
        assertNotPlan(sql, pattern);
    }

    @Test
    public void testRemoveIdentityCastImplicitJsonData()
    {
        @Language("SQL") String sql = "SELECT CAST(json '{}' AS json)";
        PlanMatchPattern pattern = anyTree(
                project(anyTree()).withAssignment("Cast")
        );
        assertNotPlan(sql, pattern);
    }

    @Test
    public void testRemoveIdentityCastImplicitDateData()
    {
        @Language("SQL") String sql = "SELECT CAST(date '2011-08-22' AS date)";
        PlanMatchPattern pattern = anyTree(
                project(anyTree()).withAssignment("Cast")
        );
        assertNotPlan(sql, pattern);
    }

    @Test
    public void testRemoveIdentityCastImplicitTimeData()
    {
        @Language("SQL") String sql = "SELECT CAST(time '01:02:03.456 America/Los_Angeles' AS time)";
        PlanMatchPattern pattern = anyTree(
                project(anyTree()).withAssignment("Cast")
        );
        assertPlan(sql, pattern);
    }

    @Test
    public void testRemoveIdentityCastImplicitTimestampData()
    {
        @Language("SQL") String sql = "SELECT CAST(timestamp '2001-08-22 03:04:05.321 America/Los_Angeles' AS timestamp)";
        PlanMatchPattern pattern = anyTree(
                project(anyTree()).withAssignment("Cast")
        );
        assertNotPlan(sql, pattern);
    }

    @Test
    public void testRemoveIdentityCastImplicitArrayData()
    {
        @Language("SQL") String sql = "SELECT CAST(array[1,2,3] AS array(bigint))";
        PlanMatchPattern pattern = anyTree(
                project(anyTree()).withAssignment("Cast")
        );
        assertNotPlan(sql, pattern);
    }

    @Test
    public void testRemoveIdentityCastImplicitMapData()
    {
        @Language("SQL") String sql = "SELECT CAST(map(array['foo','bar'], array[1,2]) as map(varchar,bigint))";
        PlanMatchPattern pattern = anyTree(
                project(anyTree()).withAssignment("Cast")
        );
        assertNotPlan(sql, pattern);
    }

    private void assertNotPlan(@Language("SQL") String sql, PlanMatchPattern pattern)
    {
        Plan actualPlan = plan(sql);
        queryRunner.inTransaction(transactionSession -> {
            PlanAssert.assertNotPlan(transactionSession, queryRunner.getMetadata(), actualPlan, pattern);
            return null;
        });
    }

    private void assertPlan(@Language("SQL") String sql, PlanMatchPattern pattern)
    {
        Plan actualPlan = plan(sql);
        queryRunner.inTransaction(transactionSession -> {
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), actualPlan, pattern);
            return null;
        });
    }

    private Plan plan(@Language("SQL") String sql)
    {
        return queryRunner.inTransaction(transactionSession -> queryRunner.createPlan(transactionSession, sql));
    }
}
