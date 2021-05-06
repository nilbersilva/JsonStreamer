# JsonStreamer
AspNetCore.JsonStreamer is a library that provides support for json data streaming to ASP.NET Core MVC.

## JsonStreamActionResult
This class can be used as a return value of an ASP .NET MVC IActionResult, it will disable buffering and allow streaming of the Response.

Example will in AspNetCore Application will be added soon.

Example using the class can be seen on the **JsonStreamerTests** Project - Method Test_JsonStream

## DataSetJsonStream
This Class will stream a System.Data.DataSet as JSON UTF8 which can be Deserialized using the Async Method **DataSetJsonStreamDeserializer.DeserializeStream**

This Deserializer Suports Lists of the following types:
- Primitive Types (bool, int, long, double, single, etc)
- String
- Decimal

Example using the class can be seen on the **JsonStreamerTests** Project - Method Test_DataSetStream_DataSetJsonStreamDeserializer / Test_DataSetStream_DeserializeDataSetJson
