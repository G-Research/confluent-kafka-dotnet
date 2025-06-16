using System;
using Confluent.Kafka.Impl;
using Confluent.Kafka.Internal;

namespace Confluent.Kafka;

/// <summary>
/// Experimental alloc-free APIs.
/// Note that these APIs can be changed substantially or removed entirely in the future versions.
/// </summary>
public abstract class Experimental
{
    /// <summary>
    /// Callback delegate for allocation-free message consumption.
    /// </summary>
    /// <typeparam name="TKey">The key type</typeparam>
    /// <typeparam name="TValue">The value type</typeparam>
    /// <param name="reader">Message reader providing allocation-free access to message data</param>
    public delegate void AllocFreeConsumeCallback(in MessageReader reader);

    /// <summary>
    /// Alloc-free delivery handler
    /// </summary>
    public delegate void AllocFreeDeliveryHandler(in MessageReader arg);

    /// <summary>
    /// Allocation-free message reader that provides access to consumed message data.
    /// This is a ref struct to ensure stack allocation and prevent escaping the callback scope.
    /// </summary>
    public readonly unsafe ref struct MessageReader
    {
        private readonly rd_kafka_message* msg;
        private readonly Lazy<IntPtr> _hdrsPtr;

        internal MessageReader(
            rd_kafka_message* msg)
        {
            this.msg = msg;
            _hdrsPtr = new Lazy<IntPtr>(() =>
            {
                var ptr = (IntPtr)msg;
                IntPtr hdrsPtr;
                Librdkafka.message_headers(ptr, out hdrsPtr);
                return hdrsPtr;
            });
        }

        /// <summary>Gets the topic name</summary>
        public string Topic =>
            msg->rkt != IntPtr.Zero
                ? Util.Marshal.PtrToStringUTF8(Librdkafka.topic_name(msg->rkt))
                : null;

        /// <summary>Gets the partition</summary>
        public Partition Partition => msg->partition;

        /// <summary>Gets the error code</summary>
        public ErrorCode ErrorCode => msg->err;

        /// <summary>Gets the offset</summary>
        public Offset Offset => msg->offset;

        /// <summary>Gets the leader epoch</summary>
        public int? LeaderEpoch =>
            msg->rkt != IntPtr.Zero && msg->offset != Offset.Unset
                ? Librdkafka.message_leader_epoch((IntPtr)msg)
                : null;

        /// <summary>Gets the timestamp</summary>
        public Timestamp Timestamp
        {
            get
            {
                var timestampUnix = Librdkafka.message_timestamp((IntPtr)msg, out var timestampType);
                return new Timestamp(timestampUnix, (TimestampType)timestampType);
            }
        }

        /// <summary>Gets whether this indicates partition EOF</summary>
        public bool IsPartitionEOF => msg->err == ErrorCode.Local_PartitionEOF;

        /// <summary>
        /// Gets the raw key data as a ReadOnlySpan without allocation
        /// </summary>
        public ReadOnlySpan<byte> KeySpan =>
            msg->key == IntPtr.Zero
                ? ReadOnlySpan<byte>.Empty
                : new ReadOnlySpan<byte>(msg->key.ToPointer(), (int)msg->key_len);

        /// <summary>
        /// Gets the raw value data as a ReadOnlySpan without allocation
        /// </summary>
        public ReadOnlySpan<byte> ValueSpan =>
            msg->val == IntPtr.Zero
                ? ReadOnlySpan<byte>.Empty
                : new ReadOnlySpan<byte>(msg->val.ToPointer(), (int)msg->len);

        /// <summary>
        /// Gets a header by index without allocation
        /// </summary>
        /// <param name="index">Zero-based header index</param>
        /// <param name="name">Header name (allocated string)</param>
        /// <param name="value">Header value as ReadOnlySpan</param>
        /// <returns>True if header exists at the given index</returns>
        public bool TryGetHeader(int index, out string name, out ReadOnlySpan<byte> value)
        {
            name = null;
            value = ReadOnlySpan<byte>.Empty;

            var hdrsPtr = _hdrsPtr.Value;
            if (hdrsPtr == IntPtr.Zero)
                return false;

            var err = Librdkafka.header_get_all(
                hdrsPtr,
                (IntPtr)index,
                out IntPtr namep,
                out IntPtr valuep,
                out IntPtr sizep
            );

            if (err != ErrorCode.NoError)
                return false;

            name = Util.Marshal.PtrToStringUTF8(namep);

            if (valuep != IntPtr.Zero)
            {
                value = new ReadOnlySpan<byte>(valuep.ToPointer(), (int)sizep);
            }

            return true;
        }
    }
}