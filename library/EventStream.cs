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

    public static (Int64 Length, List<Record> Records) Decode(Stream stream)
    {
        var length = 0; // The data could contain a partial record on the end, so we need to calculate what we're actually used here
        var records = new List<Record>();

        // TODO: Opportunity to do smarter handling here by _not_ loading everything into RAM in one go
        using var reader = new StreamReader(stream, Encoding.UTF8, false, -1, true);
        var raw = reader.ReadToEnd();
        var lines = raw.Split(RecordSeparator);

        for (var i = 0; i < lines.Length - 1; i++) // We do not want to process the last line. By contention, it's either incomplete or empty
        {
            var line = lines[i];
            length += line.Length + 1;
            var tokens = line.Split(FieldSeparator);
            if (tokens.Length != 3) throw new EventParseException("Event does not have exactly three tokens");

            var at = DateTime.ParseExact(tokens[0], DateFormatString, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal);
            var eventType = new EventType(tokens[1]);
            var deserializationType = eventType.TryDecode() ?? typeof(Object);
            var evt = JsonSerializer.Deserialize(tokens[2], deserializationType, SerializationOptions) ?? throw new NeverException();

            records.Add(new
            (
                at,
                eventType,
                evt
            ));
        }

        return new(length, records);
    }
}