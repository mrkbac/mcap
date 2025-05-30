from io import BytesIO

from mcap_protobuf.decoder import DecoderFactory

from mcap.reader import make_reader

from .generate import (
    generate_sample_data,
    generate_sample_data_with_disordered_proto_fds,
)


def test_protobuf_decoder():
    output = BytesIO()
    generate_sample_data(output)

    decoder = DecoderFactory()
    reader = make_reader(output)
    count = 0
    for schema, channel, message in reader.iter_messages():
        proto_msg = decoder.decoder_for("protobuf", schema)(message.data)
        count += 1
        if channel.topic == "/complex_message":
            assert proto_msg.intermediate1.simple.data.startswith("Field A")
            assert proto_msg.intermediate2.simple.data.startswith("Field B")
        elif channel.topic == "/nested_type_message":
            assert proto_msg.nested_type_message_1.doubly_nested_type_message.data.startswith(
                "Field C"
            )
            assert proto_msg.nested_type_message_2.data.startswith("Field D")
        elif channel.topic == "/simple_message":
            assert proto_msg.data.startswith("Hello MCAP protobuf world")
        else:
            raise AssertionError(f"unrecognized topic {channel.topic}")

    assert count == 30


def test_with_disordered_file_descriptors():
    output = BytesIO()
    generate_sample_data_with_disordered_proto_fds(output)
    decoder = DecoderFactory()
    reader = make_reader(output, decoder_factories=[decoder])
    for schema, channel, msg, decoded_msg in reader.iter_decoded_messages():
        assert schema is not None and schema.name == "test_proto.ComplexMessage"
        assert channel.topic == "/complex_msgs"
        assert msg.log_time == 0
        assert decoded_msg.intermediate1.simple.data == "a"
        assert decoded_msg.intermediate2.simple.data == "b"


def test_decode_twice():
    output = BytesIO()
    generate_sample_data(output)
    # ensure that two decoders can exist and decode the same set of schemas
    # without failing with "A file with this name is already in the pool.".
    decoder_1 = DecoderFactory()
    decoder_2 = DecoderFactory()
    reader = make_reader(output)
    for schema, _, message in reader.iter_messages():
        decoder_1.decoder_for("protobuf", schema)(message.data)
        decoder_2.decoder_for("protobuf", schema)(message.data)
