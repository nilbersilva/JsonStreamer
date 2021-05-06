using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace JsonStreamer
{
    public class DataSetJsonStream
    {
        public static async Task WriteToStream(Stream StreamToWrite, DataSet DS, CancellationToken cancellationToken, JsonWriterOptions JsonWriterOptions)
        {
            using (System.Text.Json.Utf8JsonWriter writter = new System.Text.Json.Utf8JsonWriter(StreamToWrite, JsonWriterOptions))
            {
                writter.WriteStartObject();

                writter.WriteStartArray("Tables");

                if (DS.Tables.Count > 0)
                {
                    foreach (DataTable table in DS.Tables)
                    {
                        writter.WriteStartObject();

                        writter.WriteString("TableName", table.TableName);

                        writter.WriteStartArray("Columns");
                        foreach (DataColumn column in table.Columns)
                        {
                            writter.WriteStartArray();
                            JsonSerializer.Serialize(writter, column.ColumnName);
                            JsonSerializer.Serialize(writter, ColumnType(column));
                            writter.WriteEndArray();
                        }
                        writter.WriteEndArray();

                        writter.WriteStartArray("Rows");

                        await writter.FlushAsync(cancellationToken);
                                     
                        if (table.Rows.Count > 0)
                        {
                            foreach (DataRow row in table.Rows)
                            {
                                writter.WriteStartArray();
                                foreach (var item in row.ItemArray)
                                {
                                    JsonSerializer.Serialize(writter, item);
                                }
                                writter.WriteEndArray();;
                            }
                        }

                        writter.WriteEndArray();

                        writter.WriteEndObject();
                    }
                }

                writter.WriteEndArray();
                writter.WriteEndObject();
            }

            await StreamToWrite.FlushAsync();
        }

        private static string ColumnType(DataColumn column)
        {
            bool bArray = false;
            var childType = column.DataType;
            if (childType.IsArray)
            {
                childType = childType.GetElementType();
                bArray = true;
            }
            else if (childType.IsGenericType)
            {
                var typeInfo = childType.GetTypeInfo();
                bool implementsGenericIEnumerableOrIsGenericIEnumerable =
                    typeInfo.ImplementedInterfaces.Any(ti => ti.IsGenericType && ti.GetGenericTypeDefinition() == typeof(IEnumerable<>)) ||
                    typeInfo.GetGenericTypeDefinition() == typeof(IEnumerable<>);

                if (implementsGenericIEnumerableOrIsGenericIEnumerable)
                {
                    childType = childType.GetGenericArguments()[0];
                    bArray = true;
                }                
            }

            if (bArray)
            {
                return $"List<{childType.FullName}>";
            }
            else
            {
                return column.DataType.FullName;
            }
        }
    }
}
