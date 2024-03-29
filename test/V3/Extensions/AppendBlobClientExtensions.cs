using System.Text;
using Azure.Storage.Blobs.Specialized;

namespace ServcoX.EventSauce.Tests.V3.Extensions;

public static class AppendBlobClientExtensions
{
    public static void AppendBlock(this AppendBlobClient target, String buffer)
    {
        ArgumentNullException.ThrowIfNull(target);

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(buffer));
        target.AppendBlock(stream);
    }
}