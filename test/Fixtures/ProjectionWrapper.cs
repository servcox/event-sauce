using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using FluentAssertions;
using ServcoX.EventSauce.Configurations;
using ServcoX.EventSauce.Extensions;

// ReSharper disable MemberCanBePrivate.Global

namespace ServcoX.EventSauce.Tests.Fixtures;

public sealed class ProjectionWrapper : IDisposable
{
    private const String ConnectionString = "UseDevelopmentStorage=true;";
    public BlobContainerClient Container { get; }
    public EventStore EventStore { get; }
    public ProjectionStore Sut { get; }
    private readonly String _aggregateName;

    public const Int32 MaxBlocksPerSlice = 10;
    
    public readonly String AggregateId1 = NewId();
    public readonly String AggregateId2 = NewId();
    private readonly String _projectionId;
    
    public ProjectionWrapper(Action<ProjectionStoreConfiguration>? builder = null)
    {
        var containerName = "unit-tests";
        Container = new(ConnectionString, containerName);
        Container.CreateIfNotExists();

        Int64 version = 1;
        _projectionId = ProjectionId.Compute(typeof(Cake), version);
        _aggregateName = Guid.NewGuid().ToString("N").ToUpperInvariant();
        EventStore = new(_aggregateName, Container, cfg => { cfg.UseTargetBlocksPerSlice(MaxBlocksPerSlice); });
        Sut = new(EventStore, cfg =>
        {
            cfg.DefineProjection<Cake>(version: version, b => b
                .OnCreation((projection, id) => projection.Id = id)
                .OnEvent<CakeBaked>((projection, body, _) => { })
                .OnEvent<CakeIced>((projection, body, _) => projection.Color = body.Color)
                .OnEvent<CakeCut>((projection, body, _) => projection.Slices += body.Slices)
                .OnUnexpectedEvent((projection, _) => projection.UnexpectedEvents++)
                .OnAnyEvent((projection, evt) =>
                {
                    projection.AnyEvents++;
                    projection.LastUpdatedAt = evt.At;
                })
                .IndexField(nameof(Cake.Color))
            );
            builder?.Invoke(cfg);
        });
    }

    public async Task PopulateTestData()
    {
        await EventStore.WriteEvent(AggregateId1, new CakeBaked());
        await EventStore.WriteEvent(AggregateId1, new CakeIced { Color = "BLUE" });
        await EventStore.WriteEvent(AggregateId1, new CakeCut { Slices = 3 });
        await EventStore.WriteEvent(AggregateId1, new CakeCut { Slices = 1 });
        await EventStore.WriteEvent(AggregateId1, new CakeBinned());
        
        await EventStore.WriteEvent(AggregateId2, new CakeBaked());
        await EventStore.WriteEvent(AggregateId2, new CakeIced { Color = "GREEN" });
        await EventStore.WriteEvent(AggregateId2, new CakeCut { Slices = 1 });
    }

    public void Assert1(Cake projection)
    {
        projection.Color.Should().Be("BLUE");
        projection.Slices.Should().Be(4);
        projection.AnyEvents.Should().Be(5);
        projection.UnexpectedEvents.Should().Be(1);
        projection.LastUpdatedAt.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(5));
    }
    
    public void Assert2(Cake projection)
    {
        projection.Color.Should().Be("GREEN");
        projection.Slices.Should().Be(1);
        projection.AnyEvents.Should().Be(3);
        projection.UnexpectedEvents.Should().Be(0);
        projection.LastUpdatedAt.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(5));
    }
    
    public BlobClient GetBlobClient() =>
        Container.GetBlobClient($"{_aggregateName}/projection/{_projectionId}.bois.lz4");

    public AppendBlobClient GetSliceClient(Int64 sliceId) =>
        Container.GetAppendBlobClient($"{_aggregateName}/event/{_aggregateName}.{sliceId.ToPaddedString()}.tsv");
    
    public void Dispose()
    {
        GetBlobClient().DeleteIfExists();
        GetSliceClient(0).DeleteIfExists();
    }
    
    private static String NewId() => Guid.NewGuid().ToString("N");
}