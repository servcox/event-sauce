namespace ServcoX.EventSauce.Tests;

public static class TestEvents
{
    public static TestEvent A1 = new() { A = "a1" };
    public static TestEvent A2 = new() { A = "a2" };
    
    public readonly record struct TestEvent : IEvent
    {
        public String A { get; init; }
    }
}