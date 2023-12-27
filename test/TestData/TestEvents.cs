using ServcoX.EventSauce.Models;

namespace ServcoX.EventSauce.Tests.TestData;

public readonly record struct TestAEvent(String A) : IEventBody;

public readonly record struct TestBEvent(String B) : IEventBody;

public readonly record struct TestCEvent(String C) : IEventBody;