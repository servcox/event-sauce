using System.Globalization;

namespace ServcoX.EventSauce;

public static class RowKeyUtils
{
    private static readonly String RowKeyFormat = new('0', UInt64.MaxValue.ToString(CultureInfo.InvariantCulture).Length);

    public static String EncodeVersion(UInt64 version) => version.ToString(RowKeyFormat, CultureInfo.InvariantCulture);
}