namespace ServcoX.EventSauce.Tests;

public static class TestData
{
    public  static readonly DateTime At = new(2000, 1, 1);
    public  static readonly DateOnly AtDate = DateOnly.FromDateTime(At);
    
    public static readonly TestEventA A1 = new() { A = "a1" };
    public static readonly TestEventA A2 = new() { A = "a2" };
    public static readonly TestEventB B = new() { B = "b" };

    public static readonly String A1Raw = $"{At:yyyyMMdd\\THHmmssK}\t{typeof(TestEventA).FullName!.ToUpperInvariant()}\t{{\"A\":\"a1\"}}\n";
    public static readonly String A2Raw = $"{At:yyyyMMdd\\THHmmssK}\t{typeof(TestEventA).FullName!.ToUpperInvariant()}\t{{\"A\":\"a2\"}}\n";
    public static readonly String BRaw = $"{At:yyyyMMdd\\THHmmssK}\t{typeof(TestEventB).FullName!.ToUpperInvariant()}\t{{\"B\":\"b\"}}\n";

    public readonly record struct TestEventA : IEvent
    {
        public String A { get; init; }
    }

    public readonly record struct TestEventB : IEvent
    {
        public String B { get; init; }
    }
}