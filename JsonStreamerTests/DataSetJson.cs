using System;
using System.Collections.Generic;
using System.Text;

namespace JsonStreamerTests
{
    public class DataSetJson
    {
        public List<DataSetJsonTable> Tables { get; set; }
    }

    public class DataSetJsonTable
    {       
        public string TableName { get; set; }
        public List<List<object>> Columns { get; set; }
        public List<List<object>> Rows { get; set; }
    }
}
