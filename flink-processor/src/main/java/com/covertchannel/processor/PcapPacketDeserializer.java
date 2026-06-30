package com.covertchannel.processor;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.pcap4j.packet.namednumber.DataLinkType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class PcapPacketDeserializer implements DeserializationSchema<NetworkPacket> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PcapPacketDeserializer.class);

    @Override
    public NetworkPacket deserialize(byte[] message) throws IOException {
        try {
            if (message == null || message.length < 4) return null;
            ByteBuffer buffer = ByteBuffer.wrap(message);
            //Extract DataLinkType to instantiate the correct PacketType
            int dltVal = buffer.getInt();
            long timestamp = buffer.getLong();
            DataLinkType dlt = DataLinkType.getInstance(dltVal);

            return new NetworkPacket(Arrays.copyOfRange(message, 12, message.length), dltVal, timestamp);

        } catch (Exception e) {
            LOG.warn("Failed to deserialize packet: {}", e.getMessage());
            return null;  // Filter nulls downstream
        }

    }

    @Override
    public boolean isEndOfStream(NetworkPacket nextElement) {
        return false;
    }

    @Override
    public TypeInformation<NetworkPacket> getProducedType() {
        return TypeInformation.of(NetworkPacket.class);
    }
}

