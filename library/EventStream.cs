using System.Globalization;
using System.Text;
using System.Text.Json.Serialization;

namespace ServcoX.EventSauce;

public static class EventStream
{
    /*
     * Event records look like this:
     * {at}\t{eventType}\t{eventPayload}\n
     */

    private const String DateFormatString = @"yyyyMMdd\THHmmssK";
    private const Char FieldSeparator = '\t';
    private const Char RecordSeparator = '\n';
    private static readonly Byte[] FieldSeparatorBytes = { Convert.ToByte(FieldSeparator) };
    private static readonly Byte[] RecordSeparatorBytes = { Convert.ToByte(RecordSeparator) };

    private static readonly JsonSerializerOptions SerializationOptions = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingDefault,
    };

    public static MemoryStream Encode(IEnumerable<IEvent> events, DateTime at)
    {
        ArgumentNullException.ThrowIfNull(events);

        var stream = new MemoryStream();
        foreach (var evt in events)
        {
            if (evt is null) throw new ArgumentException("One or more events are null", nameof(events));

            var formattedAt = at.ToString(DateFormatString, CultureInfo.InvariantCulture);
            var eventType = new EventType(evt.GetType());
            var payloadEncoded = JsonSerializer.Serialize((Object)evt, SerializationOptions); // Cast as object, otherwise STJ will only serialize what it sees on IEvent (ie, no fields)

            stream.WriteAsUtf8(formattedAt);
            stream.Write(FieldSeparatorBytes);
            stream.WriteAsUtf8(eventType.Name);
            stream.Write(FieldSeparatorBytes);
            stream.WriteAsUtf8(payloadEncoded);
            stream.Write(RecordSeparatorBytes);
        }

        stream.Rewind();
        return stream;
    }

    public static List<Record> Decode(Stream stream)
    {
        var output = new List<Record>();

        using var reader = new StreamReader(stream, Encoding.UTF8, false, -1, true);

        while (reader.ReadLine() is { } line)
        {
            if (line.Length == 0) continue; // Skip blank lines - used in testing

            var tokens = line.Split(FieldSeparator);
            if (tokens.Length != 3) throw new EventParseException("Event does not have exactly three tokens");

            var at = DateTime.ParseExact(tokens[0], DateFormatString, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal);
            var eventType = new EventType(tokens[1]);
            var deserializationType = eventType.TryDecode() ?? typeof(Object);
            var evt = JsonSerializer.Deserialize(tokens[2], deserializationType, SerializationOptions) ?? throw new NeverException();

            output.Add(new
            (
                at,
                eventType,
                evt
            ));
        }

        return output;
    }
}