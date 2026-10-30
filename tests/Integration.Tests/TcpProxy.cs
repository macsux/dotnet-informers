using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Steeltoe.Informers.Integration.Tests
{
    /// <summary>A local TCP proxy to the API server whose connections can be cut (and new ones refused) to simulate network failures.</summary>
    public sealed class TcpProxy : IDisposable
    {
        private readonly TcpListener _listener = new TcpListener(IPAddress.Loopback, 0);
        private readonly string _targetHost;
        private readonly int _targetPort;
        private readonly CancellationTokenSource _stop = new CancellationTokenSource();
        private readonly ConcurrentDictionary<TcpClient, TcpClient> _connections = new ConcurrentDictionary<TcpClient, TcpClient>();
        private volatile bool _refusing;

        public TcpProxy(Uri target)
        {
            _targetHost = target.Host;
            _targetPort = target.Port;
            _listener.Start();
            _ = AcceptAsync();
        }

        public Uri Address(Uri target) => new UriBuilder(target) { Host = "127.0.0.1", Port = ((IPEndPoint)_listener.LocalEndpoint).Port }.Uri;

        /// <summary>Drops every open connection and refuses new ones until <see cref="Restore" />.</summary>
        public void Cut()
        {
            _refusing = true;
            foreach (var pair in _connections)
            {
                pair.Key.Dispose();
                pair.Value.Dispose();
            }

            _connections.Clear();
        }

        public void Restore() => _refusing = false;

        private async Task AcceptAsync()
        {
            while (!_stop.IsCancellationRequested)
            {
                TcpClient client;
                try
                {
                    client = await _listener.AcceptTcpClientAsync(_stop.Token);
                }
                catch (Exception)
                {
                    return;
                }

                if (_refusing)
                {
                    client.Dispose();
                    continue;
                }

                _ = ForwardAsync(client);
            }
        }

        private async Task ForwardAsync(TcpClient client)
        {
            var upstream = new TcpClient();
            try
            {
                await upstream.ConnectAsync(_targetHost, _targetPort, _stop.Token);
                _connections[client] = upstream;
                var a = client.GetStream().CopyToAsync(upstream.GetStream(), _stop.Token);
                var b = upstream.GetStream().CopyToAsync(client.GetStream(), _stop.Token);
                await Task.WhenAny(a, b);
            }
            catch (Exception)
            {
            }
            finally
            {
                _connections.TryRemove(client, out _);
                client.Dispose();
                upstream.Dispose();
            }
        }

        public void Dispose()
        {
            _stop.Cancel();
            _listener.Stop();
            Cut();
        }
    }
}
