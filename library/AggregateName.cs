using System.Text.RegularExpressions;
using ServcoX.EventSauce.V3.Exceptions;

namespace ServcoX.EventSauce;

public readonly record struct AggregateName
{
    private static readonly Regex Pattern = new("^[A-Z0-9-_ ]{1,64}$");
    private readonly String _underlying;

    public AggregateName(String value)
    {
        if (String.IsNullOrEmpty(value)) throw new ArgumentNullOrDefaultException(nameof(value));
        if (!Pattern.IsMatch(value)) throw new ArgumentException($"Aggregate name must match pattern '{Pattern}'", nameof(value));
        _underlying = value;
    }

    public override String ToString() => _underlying;
    public static implicit operator String(AggregateName record) => record.ToString();
}