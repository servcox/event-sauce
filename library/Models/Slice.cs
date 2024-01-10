namespace ServcoX.EventSauce.Models;

public readonly record struct Slice(Int64 Id, Int64 End, DateTime CreatedAt);