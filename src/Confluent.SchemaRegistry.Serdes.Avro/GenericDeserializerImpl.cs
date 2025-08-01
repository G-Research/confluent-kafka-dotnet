﻿// Copyright 2018 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Avro.IO;
using Avro.Generic;
using Confluent.Kafka;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;


namespace Confluent.SchemaRegistry.Serdes
{
    internal class GenericDeserializerImpl : AsyncDeserializer<GenericRecord, Avro.Schema>
    {
        /// <remarks>
        ///     A datum reader cache (one corresponding to each write schema that's been seen)
        ///     is maintained so that they only need to be constructed once.
        /// </remarks>
        private readonly Dictionary<(Avro.Schema, Avro.Schema), DatumReader<GenericRecord>> datumReaderBySchema 
            = new Dictionary<(Avro.Schema, Avro.Schema), DatumReader<GenericRecord>>();
       
        public GenericDeserializerImpl(
            ISchemaRegistryClient schemaRegistryClient, 
            AvroDeserializerConfig config,
            RuleRegistry ruleRegistry) : base(schemaRegistryClient, config, ruleRegistry)
        {
            if (config == null) { return; }

            if (config.UseLatestVersion != null) { this.useLatestVersion = config.UseLatestVersion.Value; }
            if (config.UseLatestWithMetadata != null) { this.useLatestWithMetadata = config.UseLatestWithMetadata; }
            if (config.SubjectNameStrategy != null) { this.subjectNameStrategy = config.SubjectNameStrategy.Value.ToDelegate(); }
            if (config.SchemaIdStrategy != null) { this.schemaIdDecoder = config.SchemaIdStrategy.Value.ToDeserializer(); }
        }

        public override async Task<GenericRecord> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull,
            SerializationContext context)
        {
            return isNull
                ? default
                : await Deserialize(context.Topic, context.Headers, data,
                    context.Component == MessageComponentType.Key).ConfigureAwait(false);
        }

        private async Task<GenericRecord> Deserialize(string topic, Headers headers, ReadOnlyMemory<byte> array, bool isKey)
        {
            try
            {
                // Note: topic is not necessary for deserialization (or knowing if it's a key 
                // or value) only the schema id is needed.

                string subject = GetSubjectName(topic, isKey, null);
                RegisteredSchema latestSchema = null;
                if (subject != null)
                {
                    latestSchema = await GetReaderSchema(subject)
                        .ConfigureAwait(continueOnCapturedContext: false);
                }

                Schema writerSchemaJson;
                Avro.Schema writerSchema;
                Schema readerSchemaJson;
                Avro.Schema readerSchema;
                GenericRecord data;
                IList<Migration> migrations = new List<Migration>();
                SerializationContext context = new SerializationContext(
                    isKey ? MessageComponentType.Key : MessageComponentType.Value, topic, headers);
                SchemaId writerId = new SchemaId(SchemaType.Avro);
                var payload = schemaIdDecoder.Decode(array, context, ref writerId);
                
                (writerSchemaJson, writerSchema) = await GetWriterSchema(subject, writerId).ConfigureAwait(false);
                if (subject == null)
                {
                    subject = GetSubjectName(topic, isKey, writerSchema.Fullname);
                    if (subject != null)
                    {
                        latestSchema = await GetReaderSchema(subject)
                            .ConfigureAwait(continueOnCapturedContext: false);
                    }
                }
                payload = await ExecuteRules(isKey, subject, topic, headers, RulePhase.Encoding, RuleMode.Read,
                        null, writerSchemaJson, payload, null)
                    .ContinueWith(t => t.Result is byte[] bytes ? new ReadOnlyMemory<byte>(bytes) : (ReadOnlyMemory<byte>)t.Result)
                    .ConfigureAwait(continueOnCapturedContext: false);

                if (latestSchema != null)
                {
                    migrations = await GetMigrations(subject, writerSchemaJson, latestSchema)
                        .ConfigureAwait(continueOnCapturedContext: false);
                }

                DatumReader<GenericRecord> datumReader;
                if (migrations.Count > 0)
                {
                    using (var stream = new ReadOnlyMemoryStream(payload))
                    {
                        data = new GenericReader<GenericRecord>(writerSchema, writerSchema)
                            .Read(default(GenericRecord), new BinaryDecoder(stream));
                    }

                    string jsonString;
                    using (var jsonStream = new MemoryStream())
                    {
                        GenericRecord record = data;
                        DatumWriter<object> datumWriter = new GenericDatumWriter<object>(writerSchema);

                        JsonEncoder encoder = new JsonEncoder(writerSchema, jsonStream);
                        datumWriter.Write(record, encoder);
                        encoder.Flush();

                        jsonString = Encoding.UTF8.GetString(jsonStream.ToArray());
                    }

                    JToken json = JToken.Parse(jsonString);
                    json = await ExecuteMigrations(migrations, isKey, subject, topic, headers, json)
                        .ContinueWith(t => (JToken)t.Result)
                        .ConfigureAwait(continueOnCapturedContext: false);
                    readerSchemaJson = latestSchema;
                    readerSchema = await GetParsedSchema(readerSchemaJson).ConfigureAwait(false);
                    Avro.IO.Decoder decoder = new JsonDecoder(readerSchema, json.ToString(Formatting.None));

                    datumReader = new GenericReader<GenericRecord>(readerSchema, readerSchema);
                    data = datumReader.Read(default(GenericRecord), decoder);
                }
                else
                {
                    if (latestSchema != null)
                    {
                        readerSchemaJson = latestSchema;
                        readerSchema = await GetParsedSchema(readerSchemaJson).ConfigureAwait(false);
                    }
                    else
                    {
                        readerSchemaJson = writerSchemaJson;
                        readerSchema = writerSchema;
                    }
                    datumReader = await GetDatumReader(writerSchema, readerSchema).ConfigureAwait(false);

                    using (var stream = new ReadOnlyMemoryStream(payload))
                    {
                        data = datumReader.Read(default(GenericRecord), new BinaryDecoder(stream));
                    }
                }

                FieldTransformer fieldTransformer = async (ctx, transform, message) => 
                {
                    return await AvroUtils.Transform(ctx, readerSchema, message, transform).ConfigureAwait(false);
                };
                data = await ExecuteRules(isKey, subject, topic, headers, RuleMode.Read,
                        null, readerSchemaJson, data, fieldTransformer)
                    .ContinueWith(t => (GenericRecord)t.Result)
                    .ConfigureAwait(continueOnCapturedContext: false);

                return data;
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }

        protected override Task<Avro.Schema> ParseSchema(Schema schema)
        {
            return Task.FromResult(Avro.Schema.Parse(schema.SchemaString));
        }
        
        private async Task<DatumReader<GenericRecord>> GetDatumReader(Avro.Schema writerSchema, Avro.Schema readerSchema)
        {
            DatumReader<GenericRecord> datumReader;
            await serdeMutex.WaitAsync().ConfigureAwait(continueOnCapturedContext: false);
            try
            {
                if (datumReaderBySchema.TryGetValue((writerSchema, readerSchema), out datumReader))
                {
                    return datumReader;

                }
                else
                {
                    if (datumReaderBySchema.Count > schemaRegistryClient.MaxCachedSchemas)
                    {
                        datumReaderBySchema.Clear();
                    }

                    if (readerSchema == null)
                    {
                        readerSchema = writerSchema;
                    }
                    datumReader = new GenericReader<GenericRecord>(writerSchema, readerSchema);
                    datumReaderBySchema[(writerSchema, readerSchema)] = datumReader;
                    return datumReader;
                }
            }
            finally
            {
                serdeMutex.Release();
            }
        }
    }
}
