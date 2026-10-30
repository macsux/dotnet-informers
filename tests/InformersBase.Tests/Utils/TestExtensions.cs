using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Steeltoe.Informers.InformersBase.Tests.Utils
{
    public static class TestExtensions
    {
        private static TimeSpan DefaultTimeout = TimeSpan.FromSeconds(5);

        public static async Task TimeoutIfNotDebugging(this Task task) => await task.TimeoutIfNotDebugging(DefaultTimeout);
        public static async Task TimeoutIfNotDebugging(this Task task, TimeSpan timeout)
        {
            async Task<bool> Wrapper()
            {
                await task;
                return true;
            }
            await Wrapper().TimeoutIfNotDebugging(timeout);
        }
        
        public static async Task TimeoutIfNotDebugging(this ValueTask task) => await task.AsTask().TimeoutIfNotDebugging(DefaultTimeout);
        public static async Task TimeoutIfNotDebugging(this ValueTask task, TimeSpan timeout) => await task.AsTask().TimeoutIfNotDebugging(timeout);
        public static async Task<T> TimeoutIfNotDebugging<T>(this ValueTask<T> task) => await task.AsTask().TimeoutIfNotDebugging(DefaultTimeout);
        public static async Task<T> TimeoutIfNotDebugging<T>(this ValueTask<T> task, TimeSpan timeout) => await task.AsTask().TimeoutIfNotDebugging(timeout);

        public static async Task<T> TimeoutIfNotDebugging<T>(this Task<T> task) => await task.TimeoutIfNotDebugging(DefaultTimeout);

        public static async Task<T> TimeoutIfNotDebugging<T>(this Task<T> task, TimeSpan timeout)
        {
            if (Debugger.IsAttached)
            {
                return await task;
            }

            using var timeoutCancellationTokenSource = new CancellationTokenSource();
            var completedTask = await Task.WhenAny(task, Task.Delay(timeout, timeoutCancellationTokenSource.Token));
            if (completedTask == task)
            {
                timeoutCancellationTokenSource.Cancel();
                return await task; // Very important in order to propagate exceptions
            }

            throw new TimeoutException("The operation has timed out.");
        }

        private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions { Converters = { new JsonStringEnumConverter() } };

        public static string ToJson(this object obj)
        {
            return JsonSerializer.Serialize(obj, JsonOptions);
        }

       

        public static ResourceEvent<string, TResource> ToResourceEvent<TResource>(this TResource obj, EventTypeFlags typeFlags)
        where TResource : TestResource
        {
            return new ResourceEvent<string, TResource>(typeFlags, obj.Key, obj);
        }
    }
}

