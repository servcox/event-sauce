using System.Security.Cryptography;
using ServcoX.Rfc7515C;

namespace ServcoX.EventSauce.Utilities;

public static class ProjectionIdUtilities
{
    public static String Compute(Type type, UInt32 version)
    {
        if (type is null) throw new ArgumentNullException(nameof(type));

        using var sha = SHA256.Create();
        var hash = sha.ComputeHash(type.GUID.ToByteArray())
            .Take(8)
            .ToArray();
        return $"{type.FullName}@{version}.{Rfc7515CEncoder.Encode(hash)}";
    }
}