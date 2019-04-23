using System;
using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Threading;
using Couchbase.IO.Converters;
using Couchbase.IO.Utils;

namespace Couchbase.IO
{
    /// <summary>
    /// A factory creator for <see cref="IConnection"/>s
    /// </summary>
    public class DefaultConnectionFactory<T> : IConnectionFactory<T> where T : class, IConnection
    {
        public static DefaultConnectionFactory<T> Get()
        {
            return new DefaultConnectionFactory<T>();
        }

        private DefaultConnectionFactory()
        {

        }

        private ConcurrentQueue<Item> _eaQueue = new ConcurrentQueue<Item>();

        private class Item : IDisposable
        {
            public SocketAsyncEventArgs ea;

            public ManualResetEventSlim waitHandle;

            public Item()
            {
                this.ea = new SocketAsyncEventArgs();
                this.waitHandle = new ManualResetEventSlim(false);
                this.ea.UserToken = this.waitHandle;
                this.ea.Completed += Ea_Completed;
            }

            private void Ea_Completed(object sender, SocketAsyncEventArgs e)
            {
                this.waitHandle.Set();
            }

            public void Reset()
            {
                this.waitHandle.Reset();
            }

            public void Dispose()
            {
                this.ea.Dispose();
                this.waitHandle.Dispose();
            }
        }

        private Item Acquire()
        {
            Item item;
            if (_eaQueue.TryDequeue(out item))
            {
                return item;
            }

            return new Item();
        }

        private void Release(Item item)
        {
            item.Reset();
            _eaQueue.Enqueue(item);
        }

        /// <summary>
        /// Returns a functory for creating <see cref="Connection"/> objects.
        /// </summary>
        /// <returns>A <see cref="Connection"/> based off of the <see cref="PoolConfiguration"/> of the <see cref="IConnectionPool"/>.</returns>
        public T Get(IConnectionPool<T> p, IByteConverter c, BufferAllocator b)
        {
            var socket = new Socket(p.EndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);

            Item item = null;
            try
            {
                item = this.Acquire();
                item.ea.RemoteEndPoint = p.EndPoint;

                if (socket.ConnectAsync(item.ea))
                {
                    if (!item.waitHandle.Wait(p.Configuration.ConnectTimeout))
                    {
                        socket.Dispose();
                        const int connectionTimedOut = 10060;
                        throw new SocketException(connectionTimedOut);
                    }
                }
            }
            finally
            {
                if (item != null)
                {
                    this.Release(item);
                }
            }

            if ((item.ea.SocketError != SocketError.Success) || !socket.Connected)
            {
                socket.Dispose();
                throw new SocketException((int)item.ea.SocketError);
            }

            IConnection connection;
            if (p.Configuration.UseSsl)
            {
                connection = new SslConnection(p, socket, c, b);
                connection.Authenticate();
            }
            else
            {
                connection = Activator.CreateInstance(typeof(T), p, socket, c, b) as T;
            }
            //need to be able to completely disable the feature if false - this should work
            if (p.Configuration.EnableTcpKeepAlives)
            {
                socket.SetKeepAlives(p.Configuration.EnableTcpKeepAlives,
                    p.Configuration.TcpKeepAliveTime,
                    p.Configuration.TcpKeepAliveInterval);
            }
            return connection as T;
        }

        public void Dispose()
        {
            Item item;
            while (_eaQueue.TryDequeue(out item))
            {
                item.Dispose();
            }
        }
    }
}

#region [ License information          ]

/* ************************************************************
 *
 *    @author Couchbase <info@couchbase.com>
 *    @copyright 2014 Couchbase, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 * ************************************************************/

#endregion
