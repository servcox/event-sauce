using System.Text.Json;
using ServcoX.EventSauce.Tests.Fixtures;

namespace ServcoX.EventSauce.Tests.TestData;

public static class TestPayloads
{
    public static IEventPayload A => new CakeBaked();
    public static IEventPayload B => new CakeIced { Color = "BLUE" };
    public static IEventPayload C => new CakeCut { Slices = 3 };

    public static String AEncoded => JsonSerializer.Serialize(A);
    public static String BEncoded => JsonSerializer.Serialize(B);
    public static String CEncoded => JsonSerializer.Serialize(C);
}