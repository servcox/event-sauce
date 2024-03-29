namespace ServcoX.EventSauce;

public readonly record struct Segment(DateOnly Date, Int32 Sequence, Int64 Length)
{
    public override String ToString() => $"{Date}@{Sequence} = {Length}";
}