using System.Reflection;

namespace ServcoX.EventSauce;

/// <summary>
/// Actions can't normally be stored and called generically. This makes it possible, including triggering the reflection method location early to maximise performance
/// </summary>
public sealed class GenericAction(Object action)
{
    private readonly Object _action = action ?? throw new ArgumentNullException(nameof(action));

    private readonly MethodInfo _method = action
        .GetType()
        .GetMethod(nameof(Action.Invoke)) ?? throw new ArgumentException("Does not appear to be of type Action", nameof(action)); // This call is computationally expensive

    public void Invoke(params Object[] args) => _method.Invoke(_action, args);
}