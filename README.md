# JsonStreamer
AspNetCore.JsonStreamer is a library that provides support for json data streaming to ASP.NET Core MVC.

## JsonStreamActionResult
This class can be used as a return value of an ASP .NET MVC IActionResult, it will disable buffering and allow streaming of the Response.

## DataSetJsonStream
This Class will stream a System.Data.DataSet as JSON UTF8 which can be Deserialized using the Async Method **DataSetJsonStreamDeserializer.DeserializeStream**

This Deserializer Suports Lists the following types:
- Primitive Types (bool, int, long, double, single, etc)
- String
- Decimal

Examples on the JsonStreamerTests Project
