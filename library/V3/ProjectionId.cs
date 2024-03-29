using System.Security.Cryptography;
using ServcoX.Rfc7515C;

namespace ServcoX.EventSauce.V3;

public static class ProjectionId
{
    public static String Compute(Type type, Int64 version)
    {
        if (type is null) throw new ArgumentNullException(nameof(type));
        
        var hash = SHA256.HashData(type.GUID.ToByteArray())
            .Take(8)
            .ToArray();
        return $"{type.FullName}@{version}.{Rfc7515CEncoder.Encode(hash)}";
    }
}