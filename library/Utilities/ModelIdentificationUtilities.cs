using System.Security.Cryptography;
using ServcoX.Rfc7515C;

namespace ServcoX.EventSauce.Utilities;

public static class ModelIdentificationUtilities
{
    public static String ComputeKey(Type type, UInt32 version)
    {
        ArgumentNullException.ThrowIfNull(type);
        var hash = SHA256.HashData(type.GUID.ToByteArray())
            .Take(8)
            .ToArray();
        return $"{type.FullName}.{version}.{Rfc7515CEncoder.Encode(hash)}";
    }
}