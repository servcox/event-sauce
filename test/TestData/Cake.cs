namespace ServcoX.EventSauce.Tests.TestData;

public readonly record struct CakeBaked : IEventPayload;

public readonly record struct CakeIced(String Color) : IEventPayload;

public readonly record struct CakeCut(Int32 Slices) : IEventPayload;

public readonly record struct CakeBinned : IEventPayload;

public class Cake
{
    public String Id { get; set; } = String.Empty;
    public String Color { get; set; } = String.Empty;
    public Int32 Slices { get; set; }
    public DateTime LastUpdatedAt { get; set; }
    public Int32 UnexpectedEvents { get; set; }
    public Int32 AnyEvents { get; set; }
    public Boolean HasBeenIced { get; set; }
}