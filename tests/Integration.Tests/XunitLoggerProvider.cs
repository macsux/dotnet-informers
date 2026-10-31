using System;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Steeltoe.Informers.Integration.Tests
{
    /// <summary>Writes log entries (information and above) to the test's output.</summary>
    internal sealed class XunitLoggerProvider : ILoggerProvider
    {
        private readonly ITestOutputHelper _output;

        public XunitLoggerProvider(ITestOutputHelper output) => _output = output;

        public ILogger CreateLogger(string categoryName) => new Logger(_output, categoryName);

        public void Dispose()
        {
        }

        private sealed class Logger : ILogger
        {
            private readonly ITestOutputHelper _output;
            private readonly string _category;

            public Logger(ITestOutputHelper output, string category)
            {
                _output = output;
                _category = category;
            }

            public IDisposable BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Information;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
            {
                if (!IsEnabled(logLevel))
                    return;
                try
                {
                    _output.WriteLine($"{DateTime.UtcNow:HH:mm:ss.fff} {logLevel} {_category}: {formatter(state, exception)}{(exception == null ? "" : " " + exception)}");
                }
                catch (InvalidOperationException)
                {
                    // the test already finished
                }
            }
        }
    }
}
