using System.Text.Json;
using ServcoX.EventSauce.Tests.Fixtures;

namespace ServcoX.EventSauce.Tests.TestData;

public static class TestPayloads
{
    public static Object A => new CakeBaked();
    public static Object B => new CakeIced { Color = "BLUE" };
    public static Object C => new CakeCut { Slices = 3 };

    public static String AEncoded => JsonSerializer.Serialize(A);
    public static String BEncoded => JsonSerializer.Serialize(B);
    public static String CEncoded => JsonSerializer.Serialize(C);
}