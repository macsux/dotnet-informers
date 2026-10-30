using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using static Steeltoe.Informers.InformersBase.Tests.Core.CoreTestHelpers;

namespace Steeltoe.Informers.InformersBase.Tests.Core
{
    public class CursorTests
    {
        [Fact]
        public async Task JoinBeforeFirstList_WaitsForItInsteadOfEmptyReset()
        {
            var cache = NewCache();
            var cursor = cache.Register();
            Assert.Empty(await cursor.Drain());
            cache.Replace(new[] { I(1, 10), I(2, 20) });
            Assert.Equal("ResetStart:1=10 ResetEnd:2=20", Fmt(await cursor.Drain()));
        }

        [Fact]
        public async Task EmptyList_GivesResetEmpty()
        {
            var cache = NewCache();
            cache.Replace(Array.Empty<Item>());
            var events = await cache.Register().Drain();
            Assert.Single(events);
            Assert.True(events[0].EventFlags.HasFlag(EventTypeFlags.ResetEmpty));
        }

        [Fact]
        public async Task ChangesBeforeFirstPull_AreFoldedIntoTheReset()
        {
            var cache = NewCache();
            cache.Replace(new[] { I(1, 10) });
            var cursor = cache.Register();
            cache.Upsert(I(1, 11));
            cache.Upsert(I(2, 20));
            Assert.Equal("ResetStart:1=11 ResetEnd:2=20", Fmt(await cursor.Drain()));
        }

        [Fact]
        public async Task MergesPerKey()
        {
            var cache = NewCache();
            cache.Replace(new[] { I(1, 10), I(2, 20) });
            var cursor = cache.Register();
            await cursor.Drain();

            cache.Upsert(I(3, 30)); // Add then Modify -> Add
            cache.Upsert(I(3, 31));
            cache.Upsert(I(4, 40)); // Add then Delete -> nothing
            cache.Delete(4);
            cache.Upsert(I(1, 11)); // Modify x3 -> one Modify
            cache.Upsert(I(1, 12));
            cache.Upsert(I(1, 13));
            cache.Upsert(I(2, 21)); // Modify then Delete -> Delete carrying the last sent value
            cache.Delete(2);

            Assert.Equal("Add:3=31 Modify:1=13 Delete:2=20", Fmt(await cursor.Drain()));
        }

        [Fact]
        public async Task DeleteThenReAdd_BeforePull_IsAModify_GlobalRevisionsNeverCollide()
        {
            var cache = NewCache();
            cache.Replace(new[] { I(1, 10) });
            var cursor = cache.Register();
            await cursor.Drain();
            cache.Delete(1);
            cache.Upsert(I(1, 99));
            Assert.Equal("Modify:1=99", Fmt(await cursor.Drain()));

            // Per-key revisions restarting at 1 collide with the revision last sent: the new value is never delivered.
            var broken = NewCache(fault: FaultMode.PerKeyRevision);
            broken.Replace(new[] { I(1, 10) });
            var c2 = broken.Register();
            await c2.Drain();
            broken.Delete(1);
            broken.Upsert(I(1, 99));
            Assert.Equal("", Fmt(await c2.Drain()));
        }

        [Fact]
        public async Task Relist_SendsComputedChangesOnly_NoReset()
        {
            var cache = NewCache();
            cache.Replace(new[] { I(1, 10), I(2, 20), I(3, 30) });
            var cursor = cache.Register();
            await cursor.Drain();

            // 1 is gone, 2 changed, 3 is unchanged but (without a no-op check) gets a new revision, 4 is new
            cache.Replace(new[] { I(2, 21), I(3, 30), I(4, 40) });
            Assert.Equal("Modify, Computed:2=21 Modify, Computed:3=30 Add, Computed:4=40 Delete, Computed:1=10", Fmt(await cursor.Drain()));
        }

        [Fact]
        public async Task Relist_WithNoOpCheck_UnchangedKeysSendNothing()
        {
            var cache = NewCache(EqualityComparer<Item>.Default);
            cache.Replace(new[] { I(1, 10), I(2, 20), I(3, 30) });
            var cursor = cache.Register();
            await cursor.Drain();
            cache.Replace(new[] { I(2, 21), I(3, 30), I(4, 40) });
            Assert.Equal("Modify, Computed:2=21 Add, Computed:4=40 Delete, Computed:1=10", Fmt(await cursor.Drain()));
        }

        [Fact]
        public async Task RelistDuringReset_DoesNotInterruptIt_ChangesFollowAfterResetEnd()
        {
            var cache = NewCache();
            cache.Replace(new[] { I(1, 10), I(2, 20) });
            var cursor = cache.Register();
            Assert.True(await cursor.MoveNextAsync()); // ResetStart:1 (snapshot taken)
            cache.Replace(new[] { I(2, 20), I(3, 30) });
            Assert.Equal("ResetEnd:2=20 Modify, Computed:2=20 Add, Computed:3=30 Delete, Computed:1=10", Fmt(await cursor.Drain()));
        }

        [Fact]
        public async Task ComputedFlag_ClearedByLaterWatchEvent()
        {
            var cache = NewCache();
            cache.Replace(new[] { I(1, 10) });
            var cursor = cache.Register();
            await cursor.Drain();
            cache.Replace(new[] { I(1, 11) });
            cache.Upsert(I(1, 12));
            Assert.Equal("Modify:1=12", Fmt(await cursor.Drain()));
        }

        [Fact]
        public async Task NoOpComparer_SuppressesUnchangedContent_AndChangeBack()
        {
            var cache = NewCache(EqualityComparer<Item>.Default);
            cache.Replace(new[] { I(1, 10) });
            var cursor = cache.Register();
            await cursor.Drain();
            cache.Upsert(I(1, 10));
            Assert.Empty(await cursor.Drain());
            cache.Upsert(I(1, 11)); // A -> B -> A before the consumer pulls: nothing
            cache.Upsert(I(1, 10));
            Assert.Empty(await cursor.Drain());
            cache.Upsert(I(1, 11));
            Assert.Equal("Modify:1=11", Fmt(await cursor.Drain()));
        }

        [Fact]
        public async Task WithoutNoOpCheck_ChangeBack_IsAModify()
        {
            var cache = NewCache();
            cache.Replace(new[] { I(1, 10) });
            var cursor = cache.Register();
            await cursor.Drain();
            cache.Upsert(I(1, 11));
            cache.Upsert(I(1, 10));
            Assert.Equal("Modify:1=10", Fmt(await cursor.Drain()));
        }

        [Fact]
        public async Task ProducerError_SurfacesFromEveryCursor_AfterBufferedResetItems()
        {
            var cache = NewCache();
            cache.Replace(new[] { I(1, 10), I(2, 20) });
            var a = cache.Register();
            var b = cache.Register();
            await a.Drain();
            Assert.True(await b.MoveNextAsync()); // b is in the middle of its reset
            cache.SetError(new TimeoutException("boom"));
            var ex = await Assert.ThrowsAsync<InformerException>(async () => await a.MoveNextAsync());
            Assert.IsType<TimeoutException>(ex.InnerException);
            Assert.True(await b.MoveNextAsync()); // the rest of b's reset is delivered first
            await Assert.ThrowsAsync<InformerException>(async () => await b.MoveNextAsync());
        }

        [Fact]
        public async Task Cancel_DetachesOnlyThatConsumer()
        {
            var cache = NewCache();
            cache.Replace(new[] { I(1, 10) });
            var a = cache.Register();
            var b = cache.Register();
            await a.Drain();
            await b.Drain();
            using var cts = new CancellationTokenSource();
            var waiting = a.MoveNextAsync(cts.Token).AsTask();
            cts.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting);
            await a.DisposeAsync();
            Assert.Equal(1, cache.CursorCount);
            cache.Upsert(I(1, 11));
            Assert.Equal("Modify:1=11", Fmt(await b.Drain()));
        }

        [Fact]
        public async Task Complete_DrainsThenEnds()
        {
            var cache = NewCache();
            cache.Replace(new[] { I(1, 10) });
            var cursor = cache.Register();
            cache.Upsert(I(2, 20));
            cache.Complete();
            Assert.Equal("ResetStart:1=10 ResetEnd:2=20", Fmt(await cursor.Drain()));
            Assert.False(await cursor.MoveNextAsync());
        }

        [Fact]
        public void Transform_RunsBeforeCaching()
        {
            var options = Options();
            options.Transform = x => x with { Value = x.Value * 2 };
            var cache = new SharedCache<int, Item>(options);
            cache.Replace(new[] { I(1, 10) });
            cache.Upsert(I(2, 20));
            Assert.Equal(20, cache.Snapshot()[1].Value);
            Assert.Equal(40, cache.Snapshot()[2].Value);
        }

        [Fact]
        public void MutationDetector_FailsTheProducerWhenACachedObjectChanged()
        {
            var options = new SharedInformerOptions<int, MutableItem> { KeySelector = x => x.Key, MutationFingerprint = x => $"{x.Key}:{x.Value}" };
            var cache = new SharedCache<int, MutableItem>(options);
            var item = new MutableItem { Key = 1, Value = 10 };
            cache.Replace(new[] { item });
            item.Value = 11; // a consumer modified a shared object
            Assert.Throws<InvalidOperationException>(() => cache.Upsert(new MutableItem { Key = 1, Value = 12 }));
        }

        public sealed class MutableItem
        {
            public int Key { get; set; }
            public long Value { get; set; }
        }
    }
}
