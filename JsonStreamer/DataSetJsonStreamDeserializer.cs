using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace JsonStreamer
{
    public static class DataSetJsonStreamDeserializer
    {
        public static async Task<DataSet> DeserializeStream(Stream stream, CancellationToken cancellationToken, bool leaveStreamOpen = false)
        {
            var DS = new DataSet();

            using (var streamReader = new StreamReader(stream, null, true, -1, leaveStreamOpen))
            using (var Reader = new Newtonsoft.Json.JsonTextReader(streamReader))
            {
                DataTable currentDataTable = null;
                while (await Reader.ReadAsync(cancellationToken))
                {
                    if (Reader.Depth == 3 && Reader.TokenType == Newtonsoft.Json.JsonToken.PropertyName && Reader.Value.ToString() == "TableName")
                    {
                        await Reader.ReadAsync(cancellationToken);
                        currentDataTable = new DataTable() { TableName = Reader.Value.ToString() };
                        DS.Tables.Add(currentDataTable);

                        await Reader.ReadAsync(cancellationToken);

                        if (Reader.Depth == 3 && Reader.TokenType == Newtonsoft.Json.JsonToken.PropertyName && Reader.Value.ToString() == "Columns")
                        {
                            //Start Columns Array
                            await Reader.ReadAsync(cancellationToken);

                            if (Reader.TokenType == Newtonsoft.Json.JsonToken.StartArray)
                            {
                                //Create Columns
                                while (await Reader.ReadAsync(cancellationToken) &&
                                    (Reader.TokenType != Newtonsoft.Json.JsonToken.EndArray) &&
                                    Reader.Depth > 3)
                                {
                                    if (Reader.TokenType == Newtonsoft.Json.JsonToken.StartArray) continue;
                                    if (Reader.TokenType == Newtonsoft.Json.JsonToken.EndArray) continue;

                                    if (Reader.TokenType == Newtonsoft.Json.JsonToken.String)
                                    {
                                        //First Parameter is the column Name
                                        var column = new DataColumn() { ColumnName = Reader.Value.ToString() };

                                        //Read Second Parameter which is the Type of Column
                                        await Reader.ReadAsync(cancellationToken);
                                        string sType = Reader.Value.ToString();

                                        column.DataType = Type.GetType(HandleDataColumnType(sType));
                                        currentDataTable.Columns.Add(column);

                                        //End Array
                                        await Reader.ReadAsync(cancellationToken);
                                    }
                                }
                            }
                        }

                        //End Array Columns
                        await Reader.ReadAsync(cancellationToken);

                        if (Reader.Depth == 3 && Reader.TokenType == Newtonsoft.Json.JsonToken.PropertyName && Reader.Value.ToString() == "Rows")
                        {
                            //Start Rows Array
                            await Reader.ReadAsync(cancellationToken);

                            if (Reader.TokenType == Newtonsoft.Json.JsonToken.StartArray)
                            {
                                //Start of Data Array
                                await Reader.ReadAsync(cancellationToken);

                                while (Reader.Depth > 3)
                                {
                                    if (Reader.Depth == 4 && Reader.TokenType == Newtonsoft.Json.JsonToken.StartArray)
                                    {

                                        //New Row
                                        var row = currentDataTable.NewRow();
                                        for (int i = 0; i < currentDataTable.Columns.Count; i++)
                                        {
                                            await Reader.ReadAsync(cancellationToken);

                                            bool bArray = false;
                                            var type = currentDataTable.Columns[i].DataType;
                                            if (type.IsArray)
                                            {
                                                type = type.GetElementType();
                                                bArray = true;
                                            }
                                            else if (type.IsGenericType)
                                            {
                                                var typeInfo = type.GetTypeInfo();
                                                bool implementsGenericIEnumerableOrIsGenericIEnumerable =
                                                    typeInfo.ImplementedInterfaces.Any(ti => ti.IsGenericType && ti.GetGenericTypeDefinition() == typeof(IEnumerable<>)) ||
                                                    typeInfo.GetGenericTypeDefinition() == typeof(IEnumerable<>);

                                                if (implementsGenericIEnumerableOrIsGenericIEnumerable)
                                                {
                                                    type = type.GetGenericArguments()[0];
                                                    bArray = true;
                                                }
                                            }

                                            if (bArray)
                                            {
                                                await Reader.ReadAsync(cancellationToken);
                                                while (Reader.Depth > 5)
                                                {
                                                    if (Reader.Depth == 5) break;

                                                    ReadValue(row, currentDataTable.Columns[i], Reader.Value, Reader.TokenType, true);

                                                    await Reader.ReadAsync(cancellationToken);

                                                    if (Reader.Depth == 5) break;
                                                }
                                            }
                                            else
                                            {
                                                ReadValue(row, currentDataTable.Columns[i], Reader.Value, Reader.TokenType, false);
                                            }
                                        }

                                        currentDataTable.Rows.Add(row);
                                    }

                                    await Reader.ReadAsync(cancellationToken);
                                }
                            }
                        }
                    }
                }
            }

            return DS;
        }

        private static void ReadValue(DataRow row, DataColumn column, object Value, Newtonsoft.Json.JsonToken tokenType, bool ColumnIsArray = false)
        {
            if (tokenType == Newtonsoft.Json.JsonToken.StartArray || tokenType == Newtonsoft.Json.JsonToken.StartObject || tokenType == Newtonsoft.Json.JsonToken.StartConstructor || tokenType == Newtonsoft.Json.JsonToken.EndConstructor || tokenType == Newtonsoft.Json.JsonToken.EndArray || tokenType == Newtonsoft.Json.JsonToken.EndObject) return;

            if (ColumnIsArray)
            {
                System.Collections.IList list = null;

                if (row[column] == DBNull.Value || row[column] == null)
                {
                    list = (System.Collections.IList)Activator.CreateInstance(column.DataType);
                }
                else
                {
                    list = (System.Collections.IList)row[column];
                }

                list.Add(Value);
                row[column] = list;
            }
            else
            {
                if (tokenType == Newtonsoft.Json.JsonToken.String)
                {
                    row[column] = Value ?? null;
                }
                else if (tokenType == Newtonsoft.Json.JsonToken.Date)
                {
                    row[column] = Value ?? null;
                }
                else if (tokenType == Newtonsoft.Json.JsonToken.Integer)
                {
                    row[column] = Value ?? 0;
                }
                else if (tokenType == Newtonsoft.Json.JsonToken.Float)
                {
                    row[column] = Value ?? 0;
                }
                else if (tokenType == Newtonsoft.Json.JsonToken.Boolean)
                {
                    row[column] = Value ?? false;
                }
                else
                {
                    row[column] = Value ?? null;
                }
            }
        }

        private static string HandleDataColumnType(string sType)
        {
            if (sType.Substring(0, 5) == "List<")
            {
                sType = sType.Substring(5).TrimEnd('>');
                sType = typeof(List<>).FullName + "[[" + sType + "]]";
            }
            return sType;
        }
    }
}
