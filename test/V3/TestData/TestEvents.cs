namespace ServcoX.EventSauce.Tests.V3.TestData;

public static class TestEvents
{
    public static EgressEvent A => new()
    {
        AggregateId = "1",
        Type = typeof(CakeBaked).FullName!.ToUpperInvariant(),
        Payload = TestPayloads.A,
        Metadata = TestMetadata.A,
        At = new(2000, 1, 1, 1, 1, 1),
    };

    public static EgressEvent B => new()
    {
        AggregateId = "1",
        Type = typeof(CakeIced).FullName!.ToUpperInvariant(),
        Payload = TestPayloads.B,
        Metadata = TestMetadata.B,
        At = new(2000, 1, 1, 1, 1, 1),
    };

    public static EgressEvent C => new()
    {
        AggregateId = "1",
        Type = typeof(CakeCut).FullName!.ToUpperInvariant(),
        Payload = TestPayloads.C,
        Metadata = TestMetadata.C,
        At = new(2000, 1, 1, 1, 1, 1),
    };

    public static String AEncoded => "1\t20000101T010101Z\t" + typeof(CakeBaked).FullName!.ToUpperInvariant() + "\t{}\t{\"A1-KEY\":\"A1-VALUE\",\"A2-KEY\":\"A2-VALUE\"}";
    public static String BEncoded => "1\t20000101T010101Z\t" + typeof(CakeIced).FullName!.ToUpperInvariant() + "\t{\"Color\":\"BLUE\"}\t{\"B1-KEY\":\"B1-VALUE\",\"B2-KEY\":\"B2-VALUE\"}";
    public static String CEncoded => "1\t20000101T010101Z\t" + typeof(CakeCut).FullName!.ToUpperInvariant() + "\t{\"Slices\":3}\t{\"C1-KEY\":\"C1-VALUE\",\"C2-KEY\":\"C2-VALUE\"}";
}