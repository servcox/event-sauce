using System.Globalization;

namespace ServcoX.EventSauce.Utilities;

public static class RowKeyUtilities
{
    private static readonly String RowKeyFormat = new('0', UInt64.MaxValue.ToString(CultureInfo.InvariantCulture).Length);

    public static String EncodeVersion(UInt64 version) => version.ToString(RowKeyFormat, CultureInfo.InvariantCulture);
}