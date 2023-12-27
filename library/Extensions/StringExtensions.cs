namespace ServcoX.EventSauce.Extensions;

public static class StringExtensions
{
    public static Int32 IndexOfNth(this String target, Char value, Int32 nth)
    {
        if (nth < 1) throw new ArgumentOutOfRangeException( nameof(nth), "Must be at least 1");

        var offset = -1;
        for (var i = 1; i <= nth; i++)
        {
            offset = target.IndexOf(value, offset + 1);
            if (offset == -1) return -1;
        }

        return offset;
    }
}