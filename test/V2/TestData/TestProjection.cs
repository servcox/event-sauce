namespace ServcoX.EventSauce.Tests.V2.TestData;

public record TestProjection
{
    public String Id { get; set; } = String.Empty;
    public Int32 A { get; set; }
    public Int32 B { get; set; }
    public Int32 Other { get; set; }
    public Int32 Any { get; set; }
}