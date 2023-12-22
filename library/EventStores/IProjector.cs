namespace ServcoX.EventSauce.EventStores;

public interface IProjector<T>
{
    void New();
    void Load(String streamId, T projection);
    T Unload();
    void PromiscuousApply(Event evt);
    void FallbackApply(Event evt);
}