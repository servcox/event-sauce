using System.Globalization;

namespace ServcoX.EventSauce;

public static class Int32Extensions
{
    private static readonly String PaddedFormatString = new('0', Int32.MaxValue.ToString(CultureInfo.InvariantCulture).Length);

    public static String ToPaddedString(this Int32 target) => target.ToString(PaddedFormatString, CultureInfo.InvariantCulture);
}