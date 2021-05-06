using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Infrastructure;
using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Text;
using System.Data;
using System.IO;

namespace JsonStreamer
{
    public class JsonStreamActionResult : ActionResult, IStatusCodeActionResult, IAsyncDisposable
    {
        private readonly TaskCompletionSource<bool> readyTaskCompletionSource = new TaskCompletionSource<bool>();
        private readonly TaskCompletionSource<bool> completeTaskCompletionSource = new TaskCompletionSource<bool>();
        private ActionContext _context { get; set; }
        private bool disposed = false;
        public Stream StreamToWrite { get; set; }

        public string ContentType = "application/json";
        public JsonSerializerOptions JsonSerializerOptions { get; set; }

        /// <summary>
        /// Gets or sets the HTTP status code.
        /// </summary>
        public int? StatusCode { get; set; }

        /// <summary>
        /// Executes the result operation of the action method synchronously. This method is called by the Controller to process the result of an action method.
        /// </summary>
        /// <param name="context"></param>
        public override void ExecuteResult(ActionContext context)
        {
            throw new NotSupportedException($"The {nameof(JsonStreamActionResult)} doesn't support synchronous execution.");
        }

        /// <summary>
        /// Disables MVC Buffering
        /// </summary>
        private void DisableBuffering()
        {
            if (_context == null) return;

            IHttpResponseBodyFeature responseBodyFeature = _context.HttpContext.Features.Get<IHttpResponseBodyFeature>();
            if (responseBodyFeature != null)
            {
                responseBodyFeature.DisableBuffering();
            }
        }

        /// <summary>
        /// Executes the result operation of the action method asynchronously. This method is called by the Controller to process the result of an action method.
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public override async Task ExecuteResultAsync(ActionContext context)
        {
            _context = context ?? throw new ArgumentNullException(nameof(context));

            DisableBuffering();

            if (StatusCode.HasValue)
            {
                context.HttpContext.Response.StatusCode = StatusCode.Value;
            }

            if (!string.IsNullOrWhiteSpace(ContentType))
                context.HttpContext.Response.ContentType = ContentType;

            StreamToWrite = _context.HttpContext.Response.Body;

            readyTaskCompletionSource.SetResult(true);

            await completeTaskCompletionSource.Task;
        }

        public void ForceReady()
        {
            readyTaskCompletionSource.TrySetResult(true);
        }

        /// <summary>
        /// Streams to Response Body value parameter as json string
        /// </summary>
        /// <param name="value">Value to Serialize</param>
        /// <param name="cancellationToken">Normally RequestAborted Token is used</param>
        /// <param name="bFlushStream">Await Response Body Flush Stream<param>
        /// <returns></returns>
        public async Task WriteJsonAsync(object value, CancellationToken cancellationToken, bool bFlushStream = true)
        {
            if (!readyTaskCompletionSource.Task.IsCompletedSuccessfully)
            {
                await readyTaskCompletionSource.Task;
            }

            if (value == null) return;

            await JsonSerializer.SerializeAsync(StreamToWrite, value, value.GetType(), JsonSerializerOptions, cancellationToken);
            if (bFlushStream) await FlushStream();
        }

        /// <summary>
        /// Text Will be Written using UTF8 Encoding
        /// </summary>
        /// <param name="Text">Value to Write to the Response Stream</param>
        /// <param name="cancellationToken">Normally RequestAborted Token is used</param>
        /// <param name="bFlushStream">Await Response Body Flush Stream<param>
        /// <returns>Task</returns>
        public async Task WriteTextAsync(string Text, CancellationToken cancellationToken, bool bFlushStream = true)
        {
            if (!readyTaskCompletionSource.Task.IsCompletedSuccessfully)
            {
                await readyTaskCompletionSource.Task;
            }

            await StreamToWrite.WriteAsync(Encoding.UTF8.GetBytes(Text), cancellationToken);
            if (bFlushStream) await FlushStream();
        }

        public async Task WriteDataSetAsync(DataSet DS, CancellationToken cancellationToken, bool bFlushStream = true)
        {
            if (!readyTaskCompletionSource.Task.IsCompletedSuccessfully)
            {
                await readyTaskCompletionSource.Task;
            }

            //Write DataSet Header
            ContentType = "application/jsondataset";
            _context.HttpContext.Response.ContentType = ContentType;

            bool Indented = JsonSerializerOptions?.WriteIndented ?? false;

            string dataSetHeader = "{";

            if (Indented)
            {
                dataSetHeader += "\n";
            }

            dataSetHeader += "\"Tables\":[";

            await WriteTextAsync(dataSetHeader, cancellationToken, bFlushStream);

            var lastTable = DS.Tables[DS.Tables.Count - 1];

            foreach (DataTable table in DS.Tables)
            {
                string tableHeader = "{";

                if (Indented)
                {
                    tableHeader += "\n";
                }

                tableHeader += $"\"TableName\": \"{table.TableName}\",";

                if (Indented)
                {
                    tableHeader += "\n";
                }

                tableHeader += "\"Columns\":[";
                for (int i = 0; i < table.Columns.Count; i++)
                {
                    var column = table.Columns[i];

                    tableHeader += $"[ \"{column.ColumnName}\" , \"{column.DataType.FullName}\"]";

                    //Close Columns Array
                    if (i == table.Columns.Count - 1) tableHeader += "]";

                    tableHeader += ",";

                    if (Indented)
                    {
                        tableHeader += "\n";
                    }
                }

                tableHeader += "\"Rows\":[";

                await WriteTextAsync(tableHeader, cancellationToken, bFlushStream);

                var LastRow = table.Rows[table.Rows.Count - 1];

                foreach (DataRow row in table.Rows)
                {
                    await WriteJsonAsync(row.ItemArray, cancellationToken, false);

                    //Append command before last row
                    if (row != LastRow) await WriteTextAsync(",", cancellationToken, false);

                    if (Indented)
                    {
                        await WriteTextAsync("\n", cancellationToken, bFlushStream);
                    }
                }


                if (table != lastTable)
                {
                    await WriteTextAsync("]},", cancellationToken, false);

                    if (Indented)
                    {
                        await WriteTextAsync("\n", cancellationToken, bFlushStream);
                    }
                }
                else
                {
                    await WriteTextAsync("]}", cancellationToken, false);
                }
            }

            //Write DataSet Closing Tags.
            await WriteTextAsync("]}", cancellationToken, true);
        }

        /// <summary>
        /// Flushes Response Stream
        /// </summary>
        /// <returns></returns>
        public async Task FlushStream()
        {
            if (!readyTaskCompletionSource.Task.IsCompletedSuccessfully)
            {
                await readyTaskCompletionSource.Task;
            }

            await StreamToWrite.FlushAsync();
        }

        /// <summary>
        /// Marks the Operation as Complete, no more values will be written to it.
        /// </summary>
        public void Complete()
        {
            completeTaskCompletionSource.SetResult(true);
        }

        private async Task DisposeTask()
        {
            if (!disposed)
            {
                disposed = true;
                try
                {
                    if (_context != null) await _context.HttpContext.Response.CompleteAsync();
                }
                catch (Exception)
                {
                }
                Complete();
                GC.SuppressFinalize(this);
            }
        }

        public ValueTask DisposeAsync()
        {
            return new ValueTask(DisposeTask());
        }
    }

}
