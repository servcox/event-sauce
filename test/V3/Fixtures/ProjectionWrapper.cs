using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using FluentAssertions;
using ServcoX.EventSauce.Tests.V3.TestData;

// ReSharper disable MemberCanBePrivate.Global

namespace ServcoX.EventSauce.Tests.V3.Fixtures;

public sealed class ProjectionWrapper : IDisposable
{
    private const String ConnectionString = "UseDevelopmentStorage=true;";
    public BlobContainerClient Container { get; }
    public EventSauce.V3.EventStore EventStore { get; }
    public Projection<Cake> Sut { get; }
    private readonly String _aggregateName;

    public const Int32 MaxBlocksPerSlice = 10;

    public readonly String AggregateId1 = NewId();
    public readonly String AggregateId2 = NewId();
    private readonly String _projectionId;

    public ProjectionWrapper(Action<EventSauce.V3.Configurations.EventStoreConfiguration>? storeBuilder = null, Action<ProjectionConfiguration<Cake>>? projectionBuilder = null, Boolean prePopulateData = false,
        Boolean prePopulateCache = false)
    {
        const String containerName = "unit-tests";
        Container = new(ConnectionString, containerName);
        Container.CreateIfNotExists();

        const Int64 version = 1;
        _projectionId = ProjectionId.Compute(typeof(Cake), version);
        _aggregateName = Guid.NewGuid().ToString("N").ToUpperInvariant();
        EventStore = new(Container, _aggregateName, cfg =>
        {
            cfg.UseTargetBlocksPerSlice(MaxBlocksPerSlice);
            storeBuilder?.Invoke(cfg);
        });

        if (prePopulateData) PopulateTestData().Wait();
        if (prePopulateCache) PopulateCache().Wait();

        Sut = EventStore.Project<Cake>(version: version, cfg =>
        {
            projectionBuilder?.Invoke(cfg);

            cfg
                .OnCreation((projection, id) => projection.Id = id)
                .OnEvent<CakeBaked>((projection, body, _) => { })
                .OnEvent<CakeIced>((projection, body, _) =>
                {
                    projection.Color = body.Color;
                    projection.HasBeenIced = true;
                })
                .OnEvent<CakeCut>((projection, body, _) => projection.Slices += body.Slices)
                .OnUnexpectedEvent((projection, _) => projection.UnexpectedEvents++)
                .OnAnyEvent((projection, evt) =>
                {
                    projection.AnyEvents++;
                    projection.LastUpdatedAt = evt.At;
                })
                .IndexField(nameof(Cake.Color))
                .IndexField(nameof(Cake.Slices))
                .IndexField(nameof(Cake.HasBeenIced));
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

    public async Task PopulateCache()
    {
        var blob = GetBlobClient();
        await using var stream = File.OpenRead("TestData/ServcoX.EventSauce.Tests.TestData.Cake@1.brlJHUCgB1E.json.br");
        await blob.UploadAsync(stream);
    }

    public void Assert1(Cake projection)
    {
        projection.Color.Should().Be("BLUE");
        projection.Slices.Should().Be(4);
        projection.AnyEvents.Should().Be(5);
        projection.UnexpectedEvents.Should().Be(1);
        projection.LastUpdatedAt.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(5));
        projection.LastUpdatedAt.Kind.Should().Be(DateTimeKind.Utc);
    }

    public void Assert2(Cake projection)
    {
        projection.Color.Should().Be("GREEN");
        projection.Slices.Should().Be(1);
        projection.AnyEvents.Should().Be(3);
        projection.UnexpectedEvents.Should().Be(0);
        projection.LastUpdatedAt.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(5));
        projection.LastUpdatedAt.Kind.Should().Be(DateTimeKind.Utc);
    }

    public BlobClient GetBlobClient() =>
        Container.GetBlobClient($"{_aggregateName}/projection/{_projectionId}.json.br");

    public AppendBlobClient GetSliceClient(Int64 sliceId) =>
        Container.GetAppendBlobClient($"{_aggregateName}/event/{_aggregateName}.{sliceId.ToPaddedString()}.tsv");

    public void Dispose()
    {
        GetBlobClient().DeleteIfExists();
        GetSliceClient(0).DeleteIfExists();
    }

    private static String NewId() => Guid.NewGuid().ToString("N");
}