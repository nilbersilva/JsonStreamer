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
        private System.Text.Json.Utf8JsonWriter writter { get; set; }
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

            if (writter == null)
            {
                writter = new System.Text.Json.Utf8JsonWriter(StreamToWrite, new JsonWriterOptions() { Indented = JsonSerializerOptions?.WriteIndented ?? false });
            }

            readyTaskCompletionSource.SetResult(true);

            await completeTaskCompletionSource.Task;
        }

        public void ForceReady()
        {
            if (writter == null)
            {
                writter = new System.Text.Json.Utf8JsonWriter(StreamToWrite, new JsonWriterOptions() { Indented = JsonSerializerOptions?.WriteIndented ?? false });
            }

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

            JsonSerializer.Serialize(writter, value, JsonSerializerOptions);
            
            if (bFlushStream) await FlushStream();
        }
        
        public async Task WriteString(string propertyName, string value)
        {
            if (!readyTaskCompletionSource.Task.IsCompletedSuccessfully)
            {
                await readyTaskCompletionSource.Task;
            }

            writter.WriteString(propertyName, value);
        }

        public async Task WriteString(string propertyName, DateTime value)
        {
            if (!readyTaskCompletionSource.Task.IsCompletedSuccessfully)
            {
                await readyTaskCompletionSource.Task;
            }

            writter.WriteString(propertyName, value);
        }

        public async Task WriteString(string propertyName, DateTimeOffset value)
        {
            if (!readyTaskCompletionSource.Task.IsCompletedSuccessfully)
            {
                await readyTaskCompletionSource.Task;
            }

            writter.WriteString(propertyName, value);
        }

        public async Task WriteString(string propertyName, Guid value)
        {
            if (!readyTaskCompletionSource.Task.IsCompletedSuccessfully)
            {
                await readyTaskCompletionSource.Task;
            }

            writter.WriteString(propertyName, value);
        }
        
        public async Task WriteStringValue(string value)
        {
            if (!readyTaskCompletionSource.Task.IsCompletedSuccessfully)
            {
                await readyTaskCompletionSource.Task;
            }

            writter.WriteStringValue(value);
        }

        public async Task WriteStringValue(DateTime value)
        {
            if (!readyTaskCompletionSource.Task.IsCompletedSuccessfully)
            {
                await readyTaskCompletionSource.Task;
            }

            writter.WriteStringValue(value);
        }

        public async Task WriteStringValue(DateTimeOffset value)
        {
            if (!readyTaskCompletionSource.Task.IsCompletedSuccessfully)
            {
                await readyTaskCompletionSource.Task;
            }

            writter.WriteStringValue(value);
        }

        public async Task WriteStringValue(Guid value)
        {
            if (!readyTaskCompletionSource.Task.IsCompletedSuccessfully)
            {
                await readyTaskCompletionSource.Task;
            }

            writter.WriteStringValue(value);
        }

        public async Task WriteBoolean(string propertyName, bool value)
        {
            if (!readyTaskCompletionSource.Task.IsCompletedSuccessfully)
            {
                await readyTaskCompletionSource.Task;
            }

            writter.WriteBoolean(propertyName, value);
        }

        public async Task WriteBooleanValue(bool value)
        {
            if (!readyTaskCompletionSource.Task.IsCompletedSuccessfully)
            {
                await readyTaskCompletionSource.Task;
            }

            writter.WriteBooleanValue(value);
        }
       
        public async Task WriteNumber(string propertyName, decimal value)
        {
            if (!readyTaskCompletionSource.Task.IsCompletedSuccessfully)
            {
                await readyTaskCompletionSource.Task;
            }

            writter.WriteNumber(propertyName, value);
        }

        public async Task WriteNumber(string propertyName, int value)
        {
            if (!readyTaskCompletionSource.Task.IsCompletedSuccessfully)
            {
                await readyTaskCompletionSource.Task;
            }

            writter.WriteNumber(propertyName, value);
        }

        public async Task WriteNumber(string propertyName, uint value)
        {
            if (!readyTaskCompletionSource.Task.IsCompletedSuccessfully)
            {
                await readyTaskCompletionSource.Task;
            }

            writter.WriteNumber(propertyName, value);
        }

        public async Task WriteNumber(string propertyName, float value)
        {
            if (!readyTaskCompletionSource.Task.IsCompletedSuccessfully)
            {
                await readyTaskCompletionSource.Task;
            }

            writter.WriteNumber(propertyName, value);
        }

        public async Task WriteNumber(string propertyName, long value)
        {
            if (!readyTaskCompletionSource.Task.IsCompletedSuccessfully)
            {
                await readyTaskCompletionSource.Task;
            }

            writter.WriteNumber(propertyName, value);
        }

        public async Task WriteNumber(string propertyName, ulong value)
        {
            if (!readyTaskCompletionSource.Task.IsCompletedSuccessfully)
            {
                await readyTaskCompletionSource.Task;
            }

            writter.WriteNumber(propertyName, value);
        }

        public async Task WriteNumber(string propertyName, double value)
        {
            if (!readyTaskCompletionSource.Task.IsCompletedSuccessfully)
            {
                await readyTaskCompletionSource.Task;
            }

            writter.WriteNumber(propertyName, value);
        }

        public async Task WriteStartObject()
        {
            if (!readyTaskCompletionSource.Task.IsCompletedSuccessfully)
            {
                await readyTaskCompletionSource.Task;
            }

            writter.WriteStartObject();
        }

        public async Task WriteStartObject(string propertyName)
        {
            if (!readyTaskCompletionSource.Task.IsCompletedSuccessfully)
            {
                await readyTaskCompletionSource.Task;
            }
            
            writter.WriteStartObject(propertyName);
        }

        public async Task WriteEndObject()
        {
            if (!readyTaskCompletionSource.Task.IsCompletedSuccessfully)
            {
                await readyTaskCompletionSource.Task;
            }

            writter.WriteEndObject();
        }

        public async Task WriteStartArray()
        {
            if (!readyTaskCompletionSource.Task.IsCompletedSuccessfully)
            {
                await readyTaskCompletionSource.Task;
            }

            writter.WriteStartArray();
        }

        public async Task WriteStartArray(string propertyName)
        {
            if (!readyTaskCompletionSource.Task.IsCompletedSuccessfully)
            {
                await readyTaskCompletionSource.Task;
            }

            writter.WriteStartArray(propertyName);
        }

        public async Task WriteEndArray()
        {
            if (!readyTaskCompletionSource.Task.IsCompletedSuccessfully)
            {
                await readyTaskCompletionSource.Task;
            }

            writter.WriteEndArray();
        }

        public async Task WriteNull(string propertyName)
        {
            if (!readyTaskCompletionSource.Task.IsCompletedSuccessfully)
            {
                await readyTaskCompletionSource.Task;
            }

            writter.WriteNull(propertyName);
        }

        public async Task WriteNullValue()
        {
            if (!readyTaskCompletionSource.Task.IsCompletedSuccessfully)
            {
                await readyTaskCompletionSource.Task;
            }

            writter.WriteNullValue();
        }

        public async Task WriteDataSetAsync(DataSet DS, CancellationToken cancellationToken)
        {
            if (!readyTaskCompletionSource.Task.IsCompletedSuccessfully)
            {
                await readyTaskCompletionSource.Task;
            }

            //Write DataSet Header
            ContentType = "application/jsondataset";
            _context.HttpContext.Response.ContentType = ContentType;

            await DataSetJsonStream.WriteToStream(StreamToWrite, DS, cancellationToken, new JsonWriterOptions() { Indented = this.JsonSerializerOptions?.WriteIndented ?? false });
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

            await writter.FlushAsync();
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
                {}

                if (writter != null)
                {
                    try
                    {
                        await writter.FlushAsync();                        
                    }
                    catch (Exception)
                    {}
                    try
                    {
                        await writter.DisposeAsync();
                    }
                    catch (Exception)
                    {}
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
