using JsonStreamer;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace JsonStreamerTests
{
    [TestClass]
    public class JsonStreamerTestsClass
    {
        private DataSet GetTestDataSet()
        {
            var random = new Random();

            DataSet dataSet = new DataSet();
            DataTable table = new DataTable();

            for (int i = 0; i < 2; i++)
            {
                table.Columns.Add(new DataColumn() { ColumnName = $"Col{i}" });
            }

            table.Columns.Add(new DataColumn() { ColumnName = "Num", DataType = typeof(decimal) });
            table.Columns.Add(new DataColumn() { ColumnName = "Date", DataType = typeof(DateTime) });
            table.Columns.Add(new DataColumn() { ColumnName = "List", DataType = typeof(List<string>) });
            table.Columns.Add(new DataColumn() { ColumnName = "Bool", DataType = typeof(bool) });

            for (int i = 0; i < 2; i++)
            {
                DataRow Row = table.NewRow();
                for (int n = 0; n < 2; n++)
                {
                    Row[n] = "text: " + (random.NextDouble() * 10000);
                }
                Row["Num"] = (decimal)random.NextDouble() * 10000;
                Row["Date"] = DateTime.UtcNow;
                Row["List"] = new List<string>() { $"ListItemRow{i}/1", $"ListItemRow{i}/2", $"ListItemRow{i}/3" };
                Row["Bool"] = (i % 2 == 0);

                table.Rows.Add(Row);
            }

            dataSet.Tables.Add(table);

            return dataSet;
        }


        public class TestClass
        {
            public string Name { get; set; }
            public int Age { get; set; }
        }
        [TestMethod]
        public async Task Test_JsonStream()
        {
            List<TestClass> List = new();

            using (var stream = new MemoryStream())
            {
                await using (var jsonStream = new JsonStreamActionResult())
                {
                    jsonStream.StreamToWrite = stream;
                    jsonStream.ForceReady();

                    //Json Object Start And Json Array Start
                    await jsonStream.WriteStartArray();
                    for (int i = 1; i < 11; i++)
                    {
                        var newItem = new TestClass()
                        {
                            Name = $"TestName-{i}",
                            Age = 18 + (i * 2)
                        };
                        List.Add(newItem);
                        await jsonStream.WriteJsonAsync(newItem, CancellationToken.None);
                    }
                    //Json Array End Json Object End 
                    await jsonStream.WriteEndArray();

                }

                stream.Position = 0;

                string json = System.Text.Encoding.UTF8.GetString(stream.ToArray());

                var List2 = JsonSerializer.Deserialize<List<TestClass>>(json);

                Debug.Assert(List.Count == List2.Count, "Same List Count");
                Debug.Assert(List[0].Name == List2[0].Name && List[0].Age == List2[0].Age, "Same List[0] Values");
                Debug.Assert(List[1].Name == List2[1].Name && List[1].Age == List2[1].Age, "Same List[1] Values");
                Debug.Assert(List.Last().Name == List2.Last().Name && List.Last().Age == List2.Last().Age, "Same List[Last] Values");
                Debug.Assert(json == JsonSerializer.Serialize(List), "Same Json");
            }
        }

        [TestMethod]
        public async Task Test_DataSetStream_DataSetJsonStreamDeserializer()
        {
            var dataSet = GetTestDataSet();
            var table = dataSet.Tables[0].Clone();
            table.TableName = "Table2";
            dataSet.Tables.Add(table);

            foreach (DataRow row in dataSet.Tables[0].Rows)
            {
                table.ImportRow(row);
            }

            using (var stream = new MemoryStream())
            {
                await DataSetJsonStream.WriteToStream(stream, dataSet, CancellationToken.None, new System.Text.Json.JsonWriterOptions() { Indented = true });

                stream.Position = 0;

                var dataSet2 = await DataSetJsonStreamDeserializer.DeserializeStream(stream, CancellationToken.None, true);

                Debug.Assert(dataSet.Tables.Count == dataSet2.Tables.Count, "Same Table Count");
                Debug.Assert(dataSet.Tables[0].Rows.Count == dataSet2.Tables[0].Rows.Count, "Different Table[0] Rows Count");
                Debug.Assert(dataSet.Tables[1].Rows.Count == dataSet2.Tables[1].Rows.Count, "Different Table[1] Rows Count");
                Debug.Assert(dataSet.Tables[0].Rows[0][0].Equals(dataSet2.Tables[0].Rows[0][0]), "Table[0] Row[0] Different Value");
                Debug.Assert(dataSet.Tables[1].Rows[0][0].Equals(dataSet2.Tables[1].Rows[0][0]), "Table[1] Row[0] Different Value");
                Debug.Assert(dataSet.Tables[0].Rows[0][dataSet.Tables[0].Columns.Count - 1].ToString().Equals(dataSet2.Tables[0].Rows[0][dataSet.Tables[0].Columns.Count - 1].ToString()), "Table[0] Row[Last] Different Value");
                Debug.Assert(dataSet.Tables[1].Rows[0][dataSet.Tables[0].Columns.Count - 1].ToString().Equals(dataSet2.Tables[1].Rows[0][dataSet.Tables[0].Columns.Count - 1].ToString()), "Table[1] Row[Last] Different Value");
            }
        }

        [TestMethod]
        public async Task Test_DataSetStream_DeserializeDataSetJson()
        {
            var dataSet = GetTestDataSet();
            var table = dataSet.Tables[0].Clone();
            table.TableName = "Table2";
            dataSet.Tables.Add(table);

            foreach (DataRow row in dataSet.Tables[0].Rows)
            {
                table.ImportRow(row);
            }

            using (var stream = new MemoryStream())
            {
                await DataSetJsonStream.WriteToStream(stream, dataSet, CancellationToken.None, new System.Text.Json.JsonWriterOptions() { Indented = true });

                stream.Position = 0;

                var dataSet2 = await JsonSerializer.DeserializeAsync<DataSetJson>(stream);



                Debug.Assert(dataSet.Tables.Count == dataSet2.Tables.Count, "Same Table Count");
                Debug.Assert(dataSet.Tables[0].Rows.Count == dataSet2.Tables[0].Rows.Count, "Same Table[0] Rows Count");
                Debug.Assert(dataSet.Tables[1].Rows.Count == dataSet2.Tables[1].Rows.Count, "Same Table[1] Rows Count");
                Debug.Assert(dataSet.Tables[0].Rows[0][0].Equals(dataSet2.Tables[0].Rows[0].First().ToString()), "Table[0] Row[0] Different Value");
                Debug.Assert(dataSet.Tables[1].Rows[0][0].Equals(dataSet2.Tables[1].Rows[0].First().ToString()), "Table[1] Row[0] Different Value");
                Debug.Assert(dataSet.Tables[0].Rows[0][dataSet.Tables[0].Columns.Count - 1].ToString().Equals(dataSet2.Tables[0].Rows[0][dataSet.Tables[0].Columns.Count - 1].ToString()), "Table[0] Row[Last] Different Value");
                Debug.Assert(dataSet.Tables[1].Rows[0][dataSet.Tables[0].Columns.Count - 1].ToString().Equals(dataSet2.Tables[1].Rows[0][dataSet.Tables[0].Columns.Count - 1].ToString()), "Table[1] Row[Last] Different Value");
            }
        }

    }
}
