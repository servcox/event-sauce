using ServcoX.EventSauce.Tests.Fixtures;

namespace ServcoX.EventSauce.Tests.TestData;

public static class TestEvents
{
    public static EgressEvent A => new()
    {
        Type = typeof(CakeBaked).FullName!.ToUpperInvariant(),
        Payload = TestPayloads.A,
        Metadata = TestMetadata.A,
        At = new(2000, 1, 1, 1, 1, 1),
        Slice = 0,
        Offset = 0,
        NextOffset = 0,
    };

    public static EgressEvent B => new()
    {
        Type = typeof(CakeIced).FullName!.ToUpperInvariant(),
        Payload = TestPayloads.B,
        Metadata = TestMetadata.B,
        At = new(2000, 1, 1, 1, 1, 1),
        Slice = 0,
        Offset = 0,
        NextOffset = 0,
    };

    public static EgressEvent C => new()
    {
        Type = typeof(CakeCut).FullName!.ToUpperInvariant(),
        Payload = TestPayloads.C,
        Metadata = TestMetadata.C,
        At = new(2000, 1, 1, 1, 1, 1),
        Slice = 0,
        Offset = 0,
        NextOffset = 0,
    };

    public static String AEncoded => typeof(CakeBaked).FullName!.ToUpperInvariant() + "\t20000101T010101Z\t{}\t{\"A1-KEY\":\"A1-VALUE\",\"A2-KEY\":\"A2-VALUE\"}";
    public static String BEncoded => typeof(CakeIced).FullName!.ToUpperInvariant() + "\t20000101T010101Z\t{\"Color\":\"BLUE\"}\t{\"B1-KEY\":\"B1-VALUE\",\"B2-KEY\":\"B2-VALUE\"}";
    public static String CEncoded => typeof(CakeCut).FullName!.ToUpperInvariant() + "\t20000101T010101Z\t{\"Slices\":3}\t{\"C1-KEY\":\"C1-VALUE\",\"C2-KEY\":\"C2-VALUE\"}";
}