#include <ydb/core/kqp/ut/common/columnshard.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {
Y_UNIT_TEST_SUITE(KqpOlapDelete) {
    Y_UNIT_TEST(DeleteWithDiffrentTypesPKColumns) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        auto runnerSettings = TKikimrSettings().SetAppConfig(appConfig).SetWithSampleTables(true);

        TTestHelper testHelper(runnerSettings);
        auto client = testHelper.GetKikimr().GetQueryClient();

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("time").SetType(NScheme::NTypeIds::Timestamp).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("class").SetType(NScheme::NTypeIds::Utf8).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("uniq").SetType(NScheme::NTypeIds::Utf8).SetNullable(false),
        };

        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({ "time", "class", "uniq" }).SetSchema(schema);
        testHelper.CreateTable(testTable);

        auto ts = TInstant::Now();
        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(ts.MicroSeconds()).Add("YDBAuditConnection").Add("XwdJoXyyEe+iRBpNMP91yQ==");
            testHelper.BulkUpsert(testTable, tableInserter);
        }

        testHelper.ReadData("SELECT COUNT(*) FROM `/Root/ColumnTableTest`", "[[1u]]");
        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest`", TStringBuilder() << "[[\"YDBAuditConnection\";" << ts.MicroSeconds() << "u;"
                                                                                      << "\"XwdJoXyyEe+iRBpNMP91yQ==\"]]");

        auto deleteQuery =
            TStringBuilder() << "DELETE FROM `/Root/ColumnTableTest` WHERE Cast(DateTime::MakeDate(DateTime::StartOfDay(time)) as String) == \""
                             << ts.FormatLocalTime("%Y-%m-%d")
                             << "\" and class == \"YDBAuditConnection\" and uniq = \"XwdJoXyyEe+iRBpNMP91yQ==\";";
        auto deleteQueryResult = client.ExecuteQuery(deleteQuery, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(deleteQueryResult.IsSuccess(), deleteQueryResult.GetIssues().ToString());

        auto selectQuery = TStringBuilder() << "SELECT time, class, uniq FROM `/Root/ColumnTableTest` WHERE time == CAST(\"" << ts.ToString()
                                            << "\" as Timestamp) and class == \"YDBAuditConnection\" and uniq = \"XwdJoXyyEe+iRBpNMP91yQ==\";";
        testHelper.ReadData(selectQuery, TStringBuilder() << "[]");

        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest`", "[]");
        testHelper.ReadData("SELECT COUNT(*) FROM `/Root/ColumnTableTest`", "[[0u]]");
    }
}
}
