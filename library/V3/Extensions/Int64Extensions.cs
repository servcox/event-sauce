using System.Globalization;

namespace ServcoX.EventSauce.V3.Extensions;

public static class Int64Extensions
{
    private static readonly String PaddedFormatString = new('0', Int64.MaxValue.ToString(CultureInfo.InvariantCulture).Length);

    public static String ToPaddedString(this Int64 target) => target.ToString(PaddedFormatString, CultureInfo.InvariantCulture);
}