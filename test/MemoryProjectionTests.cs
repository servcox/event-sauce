using Azure;
using Azure.Data.Tables;
using FluentAssertions;
using ServcoX.EventSauce.EventStores;
using ServcoX.EventSauce.Projections;

// ReSharper disable ClassNeverInstantiated.Global
// ReSharper disable MemberCanBePrivate.Global

namespace ServcoX.EventSauce.Tests;

public class MemoryProjectorTests
{
    private const String DevelopmentConnectionString = "UseDevelopmentStorage=true;";

    private const String UserId = "user-1";
    private const String Stream1Type = "Cake";

    private const String Stream1Id = "stream-1";
    private const String Stream1Colour = "Strawberry";
    private const Int32 Stream1Slices = 3;

    private const String Stream2Id = "stream-2";
    private const String Stream2Colour = "Blueberry";
    private const Int32 Stream2Slices = 2;

    private const String Stream2Type = "Vegetable";
    private static readonly String Stream3Id = "stream-3";
    private static readonly TimeSpan SyncInterval = TimeSpan.FromMilliseconds(100);

    [Fact]
    public async Task CanProject()
    {
        using var wrapper = new Wrapper();

        await wrapper.InjectCakeStream1();
        await wrapper.InjectVegetableStream1();
        Thread.Sleep(SyncInterval * 2);

        var projections = wrapper.Sut.Where(_ => true).ToList();
        projections.Count.Should().Be(1);
        projections[0].Colour.Should().Be(Stream1Colour);
        projections[0].Slices.Should().Be(Stream1Slices);
        projections[0].LastPersonToEat.Should().Be(null);

        await wrapper.InjectCakeStream2();
        Thread.Sleep(SyncInterval * 2);

        projections = wrapper.Sut.Where(_ => true).ToList();
        projections.Count.Should().Be(2);
        var stream1 = projections.Single(stream => stream.Id == Stream1Id);
        stream1.Id.Should().Be(Stream1Id);
        stream1.Colour.Should().Be(Stream1Colour);
        stream1.Slices.Should().Be(Stream1Slices);
        stream1.LastPersonToEat.Should().Be(null);

        var stream2 = projections.Single(stream => stream.Id == Stream2Id);
        stream2.Id.Should().Be(Stream2Id);
        stream2.Colour.Should().Be(Stream2Colour);
        stream2.Slices.Should().Be(Stream2Slices);
        stream2.LastPersonToEat.Should().Be(null);

        var eatenSlices = 1;
        await wrapper.EventStore.WriteStream(Stream2Id, new CakeSlicesEaten(eatenSlices), UserId, CancellationToken.None);
        Thread.Sleep(SyncInterval * 2);

        projections.Count.Should().Be(2);

        stream1 = projections.Single(stream => stream.Id == Stream1Id);
        stream1.Id.Should().Be(Stream1Id);
        stream1.Colour.Should().Be(Stream1Colour);
        stream1.Slices.Should().Be(Stream1Slices);
        stream1.LastPersonToEat.Should().Be(null);
        stream1.FallbackEvents.Should().Be(0);
        stream1.PromiscuousEvents.Should().Be(3);

        stream2 = projections.Single(stream => stream.Id == Stream2Id);
        stream2.Id.Should().Be(Stream2Id);
        stream2.Colour.Should().Be(Stream2Colour);
        stream2.Slices.Should().Be(Stream2Slices - eatenSlices);
        stream2.LastPersonToEat.Should().Be(UserId);
        stream2.FallbackEvents.Should().Be(0);
        stream1.PromiscuousEvents.Should().Be(3);
    }

    [Fact]
    public async Task CanHandleUnsupportedEvents() // Ie, event encountered in stream with no Apply
    {
        using var wrapper = new Wrapper();

        await wrapper.InjectCakeStream1();
        await wrapper.EventStore.WriteStream(Stream1Id, new CakeLicked(), UserId, CancellationToken.None);
        Thread.Sleep(SyncInterval * 2);

        var projection = wrapper.Sut.Where(_ => true).Single();
        projection.FallbackEvents.Should().Be(1);
    }


    [Fact]
    public async Task CanHandleUnexpectedEvents() // Ie, event encountered in stream with an unknown struct
    {
        using var wrapper = new Wrapper();

        await wrapper.InjectCakeStream1();
        await wrapper.EventTable.AddEntityAsync(new EventRecord(Stream1Id, 4, "UNEXPECTED", "{}", UserId));
        await wrapper.StreamTable.UpdateEntityAsync(new EventStreamRecord(Stream1Id, Stream1Type.ToUpperInvariant(), 4, false), ETag.All, TableUpdateMode.Replace);
        wrapper.Sut.SyncNow();
        var projection = wrapper.Sut.Where(_ => true).Single();
        projection.FallbackEvents.Should().Be(1);
    }

    [Fact]
    public async Task CanFind()
    {
        using var wrapper = new Wrapper();
        await wrapper.InjectCakeStream1();
        await wrapper.InjectCakeStream2();
        wrapper.Sut.SyncNow();
        
        wrapper.Sut.Find(Stream1Id).Id.Should().Be(Stream1Id);
    }
    
    [Fact]
    public async Task ThrowsExceptionWhenCantFind()
    {
        using var wrapper = new Wrapper();
        await wrapper.InjectCakeStream1();
        await wrapper.InjectCakeStream2();
        wrapper.Sut.SyncNow();
        
        Assert.Throws<NotFoundException>(() => wrapper.Sut.Find("bad"));
    }
    
    [Fact]
    public async Task CanTryFind()
    {
        using var wrapper = new Wrapper();
        await wrapper.InjectCakeStream1();
        await wrapper.InjectCakeStream2();
        wrapper.Sut.SyncNow();
        
        wrapper.Sut.TryFind(Stream1Id)?.Id.Should().Be(Stream1Id);
    }
    
    [Fact]
    public async Task CanReturnNullWhenCantTryFind()
    {
        using var wrapper = new Wrapper();
        await wrapper.InjectCakeStream1();
        await wrapper.InjectCakeStream2();
        wrapper.Sut.SyncNow();
        
        wrapper.Sut.TryFind("bad").Should().BeNull();
    }

    private class Wrapper : IDisposable
    {
        public EventStore EventStore { get; }
        public String Postfix { get; }
        public MemoryProjector<Cake> Sut { get; }
        public TableClient StreamTable { get; }
        public TableClient EventTable { get; }

        public Wrapper()
        {
            var postfix = Guid.NewGuid().ToString("N").ToUpperInvariant();
            var streamTableName = $"stream{postfix}";
            var eventTableName = $"event{postfix}";

            StreamTable = new(DevelopmentConnectionString, streamTableName);
            EventTable = new(DevelopmentConnectionString, eventTableName);

            EventStore = new(DevelopmentConnectionString, cfg => cfg
                .UseStreamTable(streamTableName)
                .UseEventTable(eventTableName)
                .CreateTablesIfMissing()
            );
            Sut = new(Stream1Type, EventStore, SyncInterval);
        }

        public async Task InjectCakeStream1()
        {
            await EventStore.CreateStream(Stream1Id, Stream1Type, CancellationToken.None);
            await EventStore.WriteStream(Stream1Id, new IEventBody[]
            {
                new CakeBaked(),
                new CakeIced(Stream1Colour),
                new CakeCut(Stream1Slices),
            }, UserId, CancellationToken.None);
        }

        public async Task InjectCakeStream2()
        {
            await EventStore.CreateStream(Stream2Id, Stream1Type, CancellationToken.None);
            await EventStore.WriteStream(Stream2Id, new IEventBody[]
            {
                new CakeBaked(),
                new CakeIced(Stream2Colour),
                new CakeCut(Stream2Slices),
            }, UserId, CancellationToken.None);
        }

        public async Task InjectVegetableStream1()
        {
            await EventStore.CreateStream(Stream3Id, Stream2Type, CancellationToken.None);
            await EventStore.WriteStream(Stream3Id, new IEventBody[]
            {
                new GrowVegetables(),
            }, UserId, CancellationToken.None);
        }

        public void Dispose()
        {
            Sut.Dispose();
            StreamTable.Delete();
            EventTable.Delete();
        }
    }

    public readonly record struct GrowVegetables : IEventBody;

    public readonly record struct CakeBaked : IEventBody;

    public readonly record struct CakeIced(String Colour) : IEventBody;

    public readonly record struct CakeCut(Int32 Slices) : IEventBody;

    public readonly record struct CakeSlicesEaten(Int32 Slices) : IEventBody;

    public readonly record struct CakeBinned : IEventBody;

    public readonly record struct CakeLicked : IEventBody; // This is intentionally unsupported in the following projection

    public class Cake
    {
        public String Id { get; }
        public String? Colour { get; private set; }
        public Int32 Slices { get; private set; }
        public String? LastPersonToEat { get; private set; }
        public Int32 FallbackEvents;
        public Int32 PromiscuousEvents;

        public Cake(String id)
        {
            Id = id;
        }

        public void Apply(CakeBaked body, Event evt)
        {
        }

        public void Apply(CakeIced body, Event evt) => Colour = body.Colour;

        public void Apply(CakeCut body, Event evt) => Slices += body.Slices;

        public void Apply(CakeSlicesEaten body, Event evt)
        {
            LastPersonToEat = evt.CreatedBy;
            Slices -= body.Slices;
        }

        public void Apply(CakeBinned body, Event evt) => Slices = 0;

        public void FallbackApply(Event evt) => FallbackEvents++;

        public void PromiscuousApply(Event evt) => PromiscuousEvents++;
    }
}