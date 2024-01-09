using Azure.Storage.Blobs;
using FluentAssertions;
using ServcoX.EventSauce.ConfigurationBuilders;

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
                .OnEvent<CakeIced>((projection, body, evt) => projection.Color = body.Color)
                .OnEvent<CakeCut>((projection, body, evt) => projection.Slices += body.Slices)
                .OnUnexpectedEvent((projection, evt) => projection.UnexpectedEvents++)
                .OnAnyEvent((projection, evt) => projection.AnyEvents++)
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
        await EventStore.WriteEvent(AggregateId2, new CakeIced { Color = "GREEM" });
        await EventStore.WriteEvent(AggregateId2, new CakeCut { Slices = 1 });
    }

    public void Assert1(Cake projection)
    {
        projection.Color.Should().Be("BLUE");
        projection.Slices.Should().Be(4);
        projection.AnyEvents.Should().Be(5);
        projection.UnexpectedEvents.Should().Be(1);
    }
    
    public void Assert2(Cake projection)
    {
        projection.Color.Should().Be("GREEM");
        projection.Slices.Should().Be(1);
        projection.AnyEvents.Should().Be(4);
        projection.UnexpectedEvents.Should().Be(0);
    }
    
    public BlobClient GetBlobClient(String projectionKey) =>
        Container.GetBlobClient($"{_aggregateName}/projection/{projectionKey}.bois.lz4");

    public void Dispose()
    {
        GetBlobClient(_projectionId).DeleteIfExists();
    }
    
    private static String NewId() => Guid.NewGuid().ToString("N");
}