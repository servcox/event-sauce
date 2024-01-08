using System.Text.Json;

namespace ServcoX.EventSauce.Tests.TestData;

public static class TestMetadata
{
    public static Dictionary<String, String> A => new()
    {
        ["A1-KEY"] = "A1-VALUE",
        ["A2-KEY"] = "A2-VALUE",
    };

    public static Dictionary<String, String> B => new()
    {
        ["B1-KEY"] = "B1-VALUE",
        ["B2-KEY"] = "B2-VALUE",
    };

    public static Dictionary<String, String> C => new()
    {
        ["C1-KEY"] = "C1-VALUE",
        ["C2-KEY"] = "C2-VALUE",
    };


    public static String AEncoded => JsonSerializer.Serialize(A);
    public static String BEncoded => JsonSerializer.Serialize(B);
    public static String CEncoded => JsonSerializer.Serialize(C);
}