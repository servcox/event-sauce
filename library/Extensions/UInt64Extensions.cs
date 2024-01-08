using System.Globalization;

namespace ServcoX.EventSauce.Extensions;

public static class UInt64Extensions
{
    private static readonly String PaddedFormatString = new('0', UInt64.MaxValue.ToString(CultureInfo.InvariantCulture).Length);

    public static String ToPaddedString(this UInt64 target) => target.ToString(PaddedFormatString, CultureInfo.InvariantCulture);
}