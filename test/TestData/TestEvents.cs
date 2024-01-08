namespace ServcoX.EventSauce.Tests.TestData;

public static class TestEvents
{
    public static EgressEvent<Dictionary<String, String>> A = new()
    {
        Type = "BAKEDCAKE",
        Payload = TestPayloads.A,
        Metadata = TestMetadata.A,
        At = new(2000, 1, 1, 1, 1, 1),
        Slice = 0,
        StartOffset = 0,
        EndOffset = 0,
    };

    public static EgressEvent<Dictionary<String, String>> B = new()
    {
        Type = "CAKEICED",
        Payload = TestPayloads.B,
        Metadata = TestMetadata.B,
        At = new(2000, 1, 1, 1, 1, 1),
        Slice = 0,
        StartOffset = 0,
        EndOffset = 0,
    };

    public static EgressEvent<Dictionary<String, String>> C = new()
    {
        Type = "CAKECUT",
        Payload = TestPayloads.C,
        Metadata = TestMetadata.C,
        At = new(2000, 1, 1, 1, 1, 1),
        Slice = 0,
        StartOffset = 0,
        EndOffset = 0,
    };

    public static String AEncoded = "BAKEDCAKE\t20000101T010101Z\t{}\t{\"A1-KEY\":\"A1-VALUE\",\"A2-KEY\":\"A2-VALUE\"}";
    public static String BEncoded = "CAKEICED\t20000101T010101Z\t{\"Color\":\"BLUE\"}\t{\"B1-KEY\":\"B1-VALUE\",\"B2-KEY\":\"B2-VALUE\"}";
    public static String CEncoded = "CAKECUT\t20000101T010101Z\t{\"Slices\":3}\t{\"C1-KEY\":\"C1-VALUE\",\"C2-KEY\":\"C2-VALUE\"}";
}