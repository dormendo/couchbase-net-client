using Couchbase.IO.Converters;
using System;
using System.Collections.Generic;
using System.Text;

namespace Couchbase.IO
{
    public interface IConnectionFactory<T> : IDisposable where T : class, IConnection
    {
        T Get(IConnectionPool<T> p, IByteConverter c, BufferAllocator b);
    }
}
